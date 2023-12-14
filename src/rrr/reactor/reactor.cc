
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include "../base/all.hpp"
#include "reactor.h"
#include "coroutine.h"
#include "event.h"
#include "quorum_event.h"
#include "epoll_wrapper.h"
#include "sys/times.h" 

// #define DEBUG_WAIT

namespace rrr {

const int64_t n_max_coroutine = 2000;

thread_local std::shared_ptr<Reactor> Reactor::sp_reactor_th_{};
thread_local std::shared_ptr<Reactor> Reactor::sp_disk_reactor_th_{};
thread_local std::shared_ptr<Coroutine> Reactor::sp_running_coro_th_{};
std::unordered_map<std::string, std::vector<std::shared_ptr<rrr::Pollable>>> Reactor::clients_{};
std::unordered_set<std::string> Reactor::dangling_ips_{};
//std::vector<std::shared_ptr<Event>> Reactor::disk_events_{};
//std::vector<std::shared_ptr<Event>> Reactor::ready_disk_events_{};
SpinLock Reactor::disk_job_;
SpinLock Reactor::trying_job_;

std::shared_ptr<Coroutine> Coroutine::CurrentCoroutine() {
  // TODO re-enable this verify
  verify(Reactor::sp_running_coro_th_);
  return Reactor::sp_running_coro_th_;
}

std::shared_ptr<Coroutine>
Coroutine::CreateRun(std::function<void()> func, const char* file, int64_t line) {
  auto& reactor = *Reactor::GetReactor();
  auto coro = reactor.CreateRunCoroutine(func, file, line);

	// some events might be triggered in the last coroutine.
  return coro;
}

void Coroutine::Sleep(uint64_t microseconds) {
  auto x = Reactor::CreateSpEvent<TimeoutEvent>(microseconds);
  x->Wait();
}

std::shared_ptr<Reactor>
Reactor::GetReactor() {
  if (!sp_reactor_th_) {
    Log_debug("create a coroutine scheduler");
    if (!REUSING_CORO)
      Log_warn("reusing coroutine not enabled!");
    sp_reactor_th_ = std::make_shared<Reactor>();
    sp_reactor_th_->thread_id_ = std::this_thread::get_id();
  }
  return sp_reactor_th_;
}

std::shared_ptr<Reactor>
Reactor::GetDiskReactor() {
  if (!sp_disk_reactor_th_) {
    Log_debug("create a coroutine scheduler");
    sp_disk_reactor_th_ = std::make_shared<Reactor>();
    sp_disk_reactor_th_->thread_id_ = std::this_thread::get_id();
  }
  return sp_disk_reactor_th_;
}

/**
 * @param func
 * @return
 */
std::shared_ptr<Coroutine>
Reactor::CreateRunCoroutine(const std::function<void()> func, const char *file, int64_t line) {
  std::shared_ptr<Coroutine> sp_coro;
  const bool reusing = REUSING_CORO && !available_coros_.empty();
  if (reusing) {
    n_idle_coroutines_--;
    sp_coro = available_coros_.back();
    sp_coro->id = Coroutine::global_id++;
    available_coros_.pop_back();
    verify(!sp_coro->func_);
    sp_coro->func_ = func;
  } else {
    // if (n_created_coroutines_ >= n_max_coroutine)
    //   return nullptr;
    sp_coro = std::make_shared<Coroutine>(func);
    verify(sp_coro->status_ == Coroutine::INIT);
    n_created_coroutines_++;
    if (n_created_coroutines_ % 1024 == 0) {
      Log_info("created %d, busy %d, idle %d coroutines on server %d, "
               "recent %s:%llx",
               (int)n_created_coroutines_,
               (int)n_busy_coroutines_,
               (int)n_idle_coroutines_,
               server_id_,
               file, line);
    }

  }
  n_busy_coroutines_++;
  coros_.insert(sp_coro);
  ContinueCoro(sp_coro);
//  Loop();
  return sp_coro;
}

void Reactor::CheckTimeout(std::vector<std::shared_ptr<Event>>& ready_events ) {
  auto time_now = Time::now(true);
  for (auto it = timeout_events_.begin(); it != timeout_events_.end();) {
    Event& event = **it;
    auto status = event.status_;
    switch (status) {
      case Event::INIT:
        verify(0);
      case Event::WAIT: {
        const auto &wakeup_time = event.wakeup_time_;
        verify(wakeup_time > 0);
        if (time_now > wakeup_time) {
          if (event.IsReady()) {
            // This is because our event mechanism is not perfect, some events
            // don't get triggered with arbitrary condition change.
            event.status_ = Event::READY;
          } else {
            event.status_ = Event::TIMEOUT;
          }
          ready_events.push_back(*it);
          it = timeout_events_.erase(it);
        } else {
          it++;
        }
        break;
      }
      case Event::READY:
      case Event::DONE:
        it = timeout_events_.erase(it);
        break;
      default:
        verify(0);
    }
  }

}

//  be careful this could be called from different coroutines.
void Reactor::Loop(bool infinite, bool check_timeout) {
  verify(std::this_thread::get_id() == thread_id_);
  looping_ = infinite;
	int print = 0;

  do {
    disk_job_.lock();
    if (ready_disk_events_.empty()) {
      disk_job_.unlock();
    } else {
      auto sp_event = ready_disk_events_.front();
      auto& event = *sp_event;
      ready_disk_events_.pop_front();
      disk_job_.unlock();
      auto sp_coro = event.wp_coro_.lock();
      event.status_ = Event::READY;
      if (event.status_ == Event::READY) {
        event.status_ = Event::DONE;
      }
      ContinueCoro(sp_coro);
    }

    std::vector<shared_ptr<Event>> ready_events = std::move(ready_events_);
    verify(ready_events_.empty());
#ifdef DEBUG_CHECK
    for (auto ev : ready_events) {
      verify(ev->status_ == Event::READY);
    }
#endif
    if (check_timeout) {
      CheckTimeout(ready_events);
    }

    for (auto it = ready_events.begin(); it != ready_events.end(); it++) {
      Event& event = **it;
      verify(event.status_ != Event::DONE);
      auto sp_coro = event.wp_coro_.lock();
      verify(sp_coro);
      verify(sp_coro->status_ == Coroutine::PAUSED);
      verify(coros_.find(sp_coro) != coros_.end()); // TODO ?????????
      if (event.status_ == Event::READY) {
        event.status_ = Event::DONE;
      } else {
        verify(event.status_ == Event::TIMEOUT);
      }
      #ifdef EPAXOS_PERF_TEST_CORO
      // Ignore events of coroutines that are completed: TEMPORARY FIX
      if (!*(sp_coro->up_boost_coro_task_)) {
        continue;
      }
      #endif
      ContinueCoro(sp_coro);
    }

    // FOR debug purposes.
//    auto& events = waiting_events_;
////    Log_debug("event list size: %d", events.size());
//    for (auto it = events.begin(); it != events.end(); it++) {
//      Event& event = **it;
//      const auto& status = event.status_;
//      if (event.status_ == Event::WAIT) {
//        event.Test();
//        verify(event.status_ != Event::READY);
//      }
//    }
  } while (looping_ || !ready_events_.empty() || !ready_disk_events_.empty());
  verify(ready_events_.empty());
}

void Reactor::Recycle(std::shared_ptr<Coroutine>& sp_coro) {

  // This fixes the bug that coroutines are not recycling if they don't finish immediately.
  if (REUSING_CORO) {
    sp_coro->status_ = Coroutine::RECYCLED;
    sp_coro->func_ = {};
    n_idle_coroutines_++;
    available_coros_.push_back(sp_coro);
  }
  n_busy_coroutines_--;
  coros_.erase(sp_coro);
}

void Reactor::DiskLoop(){
  
	Reactor::GetReactor()->disk_job_.lock();
  auto disk_events = Reactor::GetReactor()->disk_events_;
  auto it = Reactor::GetReactor()->disk_events_.begin();
	std::vector<std::shared_ptr<DiskEvent>> pending_disk_events_{};
  while(it != Reactor::GetReactor()->disk_events_.end()){
    auto disk_event = std::static_pointer_cast<DiskEvent>(*it);
    it = Reactor::GetReactor()->disk_events_.erase(it);
    pending_disk_events_.push_back(disk_event);
  }
  Reactor::GetReactor()->disk_job_.unlock();
	
	int total_written = 0;
	std::unordered_set<std::string> sync_set{};
	for (int i = 0; i < pending_disk_events_.size(); i++) {
		total_written += pending_disk_events_[i]->Handle();
		if (pending_disk_events_[i]->sync) {
			auto it = sync_set.find(pending_disk_events_[i]->file);
			if (it == sync_set.end()) {
				sync_set.insert(pending_disk_events_[i]->file);
			}
		}
	}

	/*struct timespec begin, end;
	clock_gettime(CLOCK_MONOTONIC, &begin);*/
	for (auto it = sync_set.begin(); it != sync_set.end(); it++) {
    int fd;
    // auto itt = Reactor::GetReactor()->opened_files_.find(*it);
    // if (itt != Reactor::GetReactor()->opened_files_.end()) {
    //   fd = fileno(itt->second);
    // } else {
		  fd = ::open(it->c_str(), O_WRONLY | O_APPEND | O_CREAT, 0777);
    // }
		::fsync(fd);
		::close(fd);
		//Log_info("reaching here");
	}
	/*clock_gettime(CLOCK_MONOTONIC, &end);
	if (total_written > 0) {
		long disk_time = (end.tv_sec - begin.tv_sec)*1000000000 + end.tv_nsec - begin.tv_nsec;
		//Log_info("time of fsync: %d", disk_time);
		//Log_info("total written: %d", total_written);

		long total_time = 0;
		long avg_time = 0;
		if (disk_count >= 100) {
			if (disk_index < 50) {
				disk_times[disk_index] = disk_time;
				disk_index++;
			} else {
				for (int i = 0; i < 49; i++) {
					disk_times[i] = disk_times[i+1];
					total_time += disk_times[i];
				}
				disk_times[49] = disk_time;
				total_time += disk_times[49];
				avg_time = total_time/disk_index;
				Log_info("time of fsync: %d", avg_time);
			}
			disk_count = 0;
		} else {
			disk_count++;
		}

		if (avg_time > 7500000) {
			Reactor::GetReactor()->slow_ = true;
		}
	}*/

	for(int i = 0; i < pending_disk_events_.size(); i++){
		Reactor::GetReactor()->disk_job_.lock();
    Reactor::GetReactor()->ready_disk_events_.push_back(pending_disk_events_[i]);
		Reactor::GetReactor()->disk_job_.unlock();
	}
}

void Reactor::ContinueCoro(std::shared_ptr<Coroutine> sp_coro) {
//  verify(!sp_running_coro_th_); // disallow nested coros
  verify(sp_running_coro_th_ != sp_coro);
  auto sp_old_coro = sp_running_coro_th_;
  sp_running_coro_th_ = sp_coro;
  verify(!sp_running_coro_th_->Finished());
  n_active_coroutines_++;

	struct timespec begin_marshal, begin_marshal_cpu, end_marshal, end_marshal_cpu;
	/*clock_gettime(CLOCK_MONOTONIC_RAW, &begin_marshal);
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &begin_marshal_cpu);*/
  //Log_info("start of %d", sp_coro->id);

	struct timespec begin, end;
	clock_gettime(CLOCK_MONOTONIC, &begin);

	if (sp_coro->status_ == Coroutine::INIT) {
    sp_coro->Run();
  } else if (sp_coro->status_ == Coroutine::PAUSED || sp_coro->status_ == Coroutine::RECYCLED) {
    // PAUSED or RECYCLED
    sp_coro->Continue();
  }

  verify(sp_running_coro_th_ == sp_coro);
  if (sp_running_coro_th_ -> Finished()) {
    Recycle(sp_coro);
  }
  sp_running_coro_th_ = sp_old_coro;
}

void Reactor::DisplayWaitingEv() {
  char buff[1000];
  int offset = 0;
  offset += sprintf(buff, "%p waiting events %d:", sp_reactor_th_.get(), waiting_events_.size());
  for (auto& it : waiting_events_) {
    offset += sprintf(buff+offset, "\n%s", it->wait_place_.c_str());
  }
  Log_info(buff);
}

// TODO PollThread -> Reactor
// TODO PollMgr -> ReactorFactory
class PollMgr::PollThread {

  struct thread_params{
    PollThread* thread;
    std::shared_ptr<Reactor> reactor_th;
  };
  friend class PollMgr;

  Epoll poll_{};

  // guard mode_ and poll_set_
  SpinLock l_;
  std::unordered_map<int, int> mode_{}; // fd->mode
  std::set<shared_ptr<Pollable>> poll_set_{};
  std::set<std::shared_ptr<Job>> set_sp_jobs_{};
  std::unordered_set<shared_ptr<Pollable>> pending_remove_{};
  SpinLock pending_remove_l_;
  SpinLock lock_job_;
  static SpinLock disk_job_;

  pthread_t th_;
  pthread_t disk_th_;
  bool stop_flag_;
  bool pause_flag_;
  bool need_disk_ = false;
  uint32_t sleep_usec_ = 0; // emulate slow

  static void* start_poll_loop(void* arg) {
    PollThread* thiz = (PollThread*) arg;
    
    //Put disk I/O thread here for now, but we should move it to clean up code
    
    struct thread_params* args = new struct thread_params;
    args->thread = thiz;
    args->reactor_th = Reactor::GetReactor();

    struct thread_params* args2 = new struct thread_params;
    args2->thread = thiz;
    args2->reactor_th = Reactor::GetReactor();

    pthread_t disk_th;
    pthread_t finalize_th;
    if (thiz->need_disk_) {
      Log_info("starting disk thread");
      Pthread_create(&disk_th, nullptr, PollMgr::PollThread::start_disk_loop, args);
    }
    
		Log_info("starting poll thread");
    thiz->poll_loop();
    delete args;
		delete args2;
    pthread_exit(nullptr);
    return nullptr;
  }

  static void* start_finalize_loop(void* arg){
    struct thread_params* args = (struct thread_params*) arg;

    PollThread* thiz =  args->thread;
    Reactor::sp_reactor_th_ = args->reactor_th;
    
    while(!thiz->stop_flag_){
			Reactor::dangling_ips_.clear();
      usleep(1*1000);
    }
    pthread_exit(nullptr);
    return nullptr;
  }

  static void* start_disk_loop(void* arg){
    struct thread_params* args = (struct thread_params*) arg;

    PollThread* thiz =  args->thread;
    Reactor::sp_reactor_th_ = args->reactor_th;
    
    while(!thiz->stop_flag_){
      Reactor::GetDiskReactor()->DiskLoop();
      sleep(0);
    }
    pthread_exit(nullptr);
    return nullptr;
  }

  void poll_loop();

  void start(PollMgr* poll_mgr) {
    Pthread_create(&th_, nullptr, PollMgr::PollThread::start_poll_loop, this);
  }

  void TriggerJob() {
    lock_job_.lock();
    auto it = set_sp_jobs_.begin();
    while (it != set_sp_jobs_.end()) {
      auto sp_job = *it;
      if (sp_job->Ready()) {
        //Log_info("Could be right before GotoNextPhase()");
        Coroutine::CreateRun([sp_job]() {sp_job->Work();}, __FILE__, __LINE__);
      }
      if (sp_job->Done()) {
        it = set_sp_jobs_.erase(it);
      } else {
        it++;
      }
    }
    lock_job_.unlock();
  }

 public:
  PollThread() : stop_flag_(false), pause_flag_(false) {
    poll_.stop = &stop_flag_;
    poll_.pause = &pause_flag_;
  }

  ~PollThread() {
    stop_flag_ = true;
    Pthread_join(th_, nullptr);

    l_.lock();
    vector<shared_ptr<Pollable>> tmp(poll_set_.begin(), poll_set_.end());
    l_.unlock();
    // when stopping, release anything registered in pollmgr
    for (auto it: tmp) {
      verify(it);
      this->remove(it);
    }
  }

  void add(shared_ptr<Pollable>);
  void remove(shared_ptr<Pollable>);
  void update_mode(shared_ptr<Pollable>, int new_mode);
  void pause() { pause_flag_ = true; }
  void resume() { pause_flag_ = false; }

  void add(std::shared_ptr<Job>);
  void remove(std::shared_ptr<Job>);
};

PollMgr::PollMgr(int n_threads /* =... */, bool need_disk)
    : n_threads_(n_threads), need_disk_(need_disk), poll_threads_() {
  verify(n_threads_ > 0);
  poll_threads_ = new PollThread[n_threads_];
  for (int i = 0; i < n_threads_; i++) {
    poll_threads_[i].need_disk_ = need_disk_;
    poll_threads_[i].start(this);
  }
}

PollMgr::~PollMgr() {
  delete[] poll_threads_;
  poll_threads_ = nullptr;
  //Log_debug("rrr::PollMgr: destroyed");
}

void PollMgr::PollThread::poll_loop() {
	std::vector<struct timespec> begins;
	long total_cpu;
	long total_time;
	int total = 0;
	int num_events = 0;
	int count = 0;
	int diff_count = 0;
	int first = 0;
	int second = 0;
	long wait_time = 0;
	long wait_cpu = 0;
	bool slow = false;
	int index = 0;

	struct timespec begin2, begin2_cpu, end2, end2_cpu, begin3, begin3_cpu, end3, end3_cpu;
	struct timespec first_begin, first_cpu;
	while (!stop_flag_) {
    TriggerJob();
    Reactor::GetReactor()->Loop(false, true);
    if (pause_flag_) {
      usleep(100000);
      continue;
    }
    if (sleep_usec_ > 0) {
      usleep(sleep_usec_);
    }
    if (!need_disk_) {
		poll_.Wait();
    } else {
		begins = poll_.Wait_One(num_events, slow);
		
		if (begins.size() == 4) {
			if (index != 0) {
				wait_time += (begins[1].tv_sec - begins[0].tv_sec)*1000000000 + (begins[1].tv_nsec - begins[0].tv_nsec);
				wait_cpu += (begins[3].tv_sec - begins[2].tv_sec)*1000000000 + (begins[3].tv_nsec - begins[2].tv_nsec);
			}
		} else {
			if (num_events >= 5) {
				if (index == 0) {
					first_begin = begins[0];
					first_cpu = begins[1];
				}
				index++;
			}
			if (index != 0) {
				wait_time += (begins[3].tv_sec - begins[2].tv_sec)*1000000000 + (begins[3].tv_nsec - begins[2].tv_nsec);
				wait_cpu += (begins[5].tv_sec - begins[4].tv_sec)*1000000000 + (begins[5].tv_nsec - begins[4].tv_nsec);
			}
		}
    poll_.Wait_Two();
    }

		if (slow) Reactor::GetReactor()->slow_ = slow;

    verify(Reactor::GetReactor()->ready_events_.empty());
    TriggerJob();
    // after each poll loop, remove uninterested pollables
    pending_remove_l_.lock();
    std::list<shared_ptr<Pollable>> remove_poll(pending_remove_.begin(), pending_remove_.end());
    pending_remove_.clear();
    pending_remove_l_.unlock();

#ifdef DEBUG_WAIT
    auto time_now = Time::now();
    if (time_now - last_time >= Time::RRR_USEC_PER_SEC) {
      // Reactor::GetReactor()->DisplayWaitingEv();
      Log_info("%p created coroutine %lu", Reactor::GetReactor().get(), Reactor::GetReactor()->n_created_coroutines_);
      last_time = time_now;
    }
#endif

    for (auto& poll: remove_poll) {
      int fd = poll->fd();

      l_.lock();
      if (mode_.find(fd) == mode_.end()) {
        // NOTE: only remove the fd when it is not immediately added again
        // if the same fd is used again, mode_ will contains its info
        poll_.Remove(poll);
      }
      l_.unlock();
    }
    TriggerJob();
    verify(Reactor::GetReactor()->ready_events_.empty());
    Reactor::GetReactor()->Loop();
    verify(Reactor::GetReactor()->ready_events_.empty());
  }
}

void PollMgr::PollThread::add(std::shared_ptr<Job> sp_job) {
  lock_job_.lock();
  set_sp_jobs_.insert(sp_job);
  lock_job_.unlock();
}

void PollMgr::PollThread::remove(std::shared_ptr<Job> sp_job) {
  lock_job_.lock();
  set_sp_jobs_.erase(sp_job);
  lock_job_.unlock();
}

void PollMgr::PollThread::add(shared_ptr<Pollable> poll) {
  int poll_mode = poll->poll_mode();
  int fd = poll->fd();
  verify(poll);

  l_.lock();

  // verify not exists
  verify(poll_set_.find(poll) == poll_set_.end());
  verify(mode_.find(fd) == mode_.end());

  // register pollable
  poll_set_.insert(poll);
  mode_[fd] = poll_mode;
  poll_.Add(poll);

  l_.unlock();
}

void PollMgr::PollThread::remove(shared_ptr<Pollable> poll) {
  bool found = false;
  l_.lock();
  auto it = poll_set_.find(poll);
  if (it != poll_set_.end()) {
    found = true;
    assert(mode_.find(poll->fd()) != mode_.end());
    poll_set_.erase(poll);
    mode_.erase(poll->fd());
  } else {
    assert(mode_.find(poll->fd()) == mode_.end());
  }
  l_.unlock();

  if (found) {
    pending_remove_l_.lock();
    pending_remove_.insert(poll);
    pending_remove_l_.unlock();
  }
}

void PollMgr::PollThread::update_mode(shared_ptr<Pollable> poll, int new_mode) {
  int fd = poll->fd();

  l_.lock();

  if (poll_set_.find(poll) == poll_set_.end()) {
    l_.unlock();
    return;
  }

  auto it = mode_.find(fd);
  verify(it != mode_.end());
  int old_mode = it->second;
  it->second = new_mode;

  if (new_mode != old_mode) {
    poll_.Update(poll, new_mode, old_mode);
  }

  l_.unlock();
}

static inline uint32_t hash_fd(uint32_t key) {
  uint32_t c2 = 0x27d4eb2d; // a prime or an odd constant
  key = (key ^ 61) ^ (key >> 16);
  key = key + (key << 3);
  key = key ^ (key >> 4);
  key = key * c2;
  key = key ^ (key >> 15);
  return key;
}

void PollMgr::add(shared_ptr<Pollable> poll) {
  int fd = poll->fd();
  if (fd >= 0) {
    int tid = hash_fd(fd) % n_threads_;
    poll_threads_[tid].add(poll);
  }
}

void PollMgr::remove(shared_ptr<Pollable> poll) {
  int fd = poll->fd();
  if (fd >= 0) {
    int tid = hash_fd(fd) % n_threads_;
    poll_threads_[tid].remove(poll);
  }
}

void PollMgr::pause() {
  for (int idx = 0; idx < n_threads_; idx++) {
    poll_threads_[idx].pause();
  }
}

void PollMgr::resume() {
  for (int idx = 0; idx < n_threads_; idx++) {
    poll_threads_[idx].sleep_usec_ = 0;
    poll_threads_[idx].resume();
  }
}

void PollMgr::slow(uint32_t sleep_usec) {
  for (int idx = 0; idx < n_threads_; idx++) {
    poll_threads_[idx].sleep_usec_ = sleep_usec;
  }
}

void PollMgr::update_mode(shared_ptr<Pollable> poll, int new_mode) {
  int fd = poll->fd();
  if (fd >= 0) {
    int tid = hash_fd(fd) % n_threads_;
    poll_threads_[tid].update_mode(poll, new_mode);
  }
}

void PollMgr::add(std::shared_ptr<Job> fjob) {
  int tid = 0;
  poll_threads_[tid].add(fjob);
}

void PollMgr::remove(std::shared_ptr<Job> fjob) {
  int tid = 0;
  poll_threads_[tid].remove(fjob);
}

} // namespace rrr
