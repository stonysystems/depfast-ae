
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
#include "epoll_wrapper.h"

namespace rrr {

thread_local std::shared_ptr<Reactor> Reactor::sp_reactor_th_{};
thread_local std::shared_ptr<Reactor> Reactor::sp_disk_reactor_th_{};
thread_local std::shared_ptr<Coroutine> Reactor::sp_running_coro_th_{};
//std::vector<std::shared_ptr<Event>> Reactor::disk_events_{};
//std::vector<std::shared_ptr<Event>> Reactor::ready_disk_events_{};
SpinLock Reactor::disk_job_;

std::shared_ptr<Coroutine> Coroutine::CurrentCoroutine() {
  // TODO re-enable this verify
  verify(Reactor::sp_running_coro_th_);
  return Reactor::sp_running_coro_th_;
}

std::shared_ptr<Coroutine>
Coroutine::CreateRun(std::function<void()> func) {
  auto& reactor = *Reactor::GetReactor();
  auto coro = reactor.CreateRunCoroutine(func);
  // some events might be triggered in the last coroutine.
  return coro;
}

std::shared_ptr<Reactor>
Reactor::GetReactor() {
  if (!sp_reactor_th_) {
    Log_debug("create a coroutine scheduler");
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
Reactor::CreateRunCoroutine(const std::function<void()> func) {
  std::shared_ptr<Coroutine> sp_coro;
  const bool reusing = REUSING_CORO && !available_coros_.empty();
  if (reusing) {
    sp_coro = available_coros_.back();
    sp_coro->id = Coroutine::global_id++;
    available_coros_.pop_back();
    verify(!sp_coro->func_);
    sp_coro->func_ = func;
  } else {
    sp_coro = std::make_shared<Coroutine>(func);
  }
  coros_.insert(sp_coro);
  ContinueCoro(sp_coro);
  Loop();
  return sp_coro;
}

//  be careful this could be called from different coroutines.
void Reactor::Loop(bool infinite) {

  verify(std::this_thread::get_id() == thread_id_);
  looping_ = infinite;

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
      verify(sp_coro);
      verify(sp_coro->status_ == Coroutine::PAUSED);
      verify(coros_.find(sp_coro) != coros_.end()); // TODO ?????????
      event.status_ = Event::READY;
      if (event.status_ == Event::READY) {
        event.status_ = Event::DONE;
      } else {
        verify(event.status_ == Event::TIMEOUT);
      }
      ContinueCoro(sp_coro);
    }

    std::vector<shared_ptr<Event>> ready_events = std::move(ready_events_);
    verify(ready_events_.empty());
    for (auto ev : ready_events) {
      verify(ev->status_ == Event::READY);
    }

    auto time_now = Time::now();
    //Log_info("Size of timeout_events_ is: %d", timeout_events_.size());
    for (auto it = timeout_events_.begin(); it != timeout_events_.end();) {
      Event& event = **it;
      auto status = event.status_;
      switch (status) {
        case Event::INIT:
          verify(0);
        case Event::WAIT: {
          //Log_info("READY???: %p", *it);
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

void Reactor::DiskLoop(){
  Reactor::GetReactor()->disk_job_.lock();
  auto disk_events = Reactor::GetReactor()->disk_events_;
  auto it = Reactor::GetReactor()->disk_events_.begin();
	std::vector<std::shared_ptr<DiskEvent>> pending_disk_events_{};
  while(it != Reactor::GetReactor()->disk_events_.end()){
    auto disk_event = std::static_pointer_cast<DiskEvent>(*it);
    //disk_event->Write();
    it = Reactor::GetReactor()->disk_events_.erase(it);
    //Reactor::GetReactor()->ready_disk_events_.push_back(disk_event);
    pending_disk_events_.push_back(disk_event);
  }
  Reactor::GetReactor()->disk_job_.unlock();
	
	for(int i = 0; i < pending_disk_events_.size(); i++){
		pending_disk_events_[i]->Write();
	}
	

	int fd = ::open("/db/data.txt", O_WRONLY | O_APPEND | O_CREAT);
	::fsync(fd);
	::close(fd);

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
  if (sp_coro->status_ == Coroutine::INIT) {
    sp_coro->Run();
  } else {
    // PAUSED or RECYCLED
    sp_coro->Continue();
  }
  verify(sp_running_coro_th_ == sp_coro);
  if (sp_running_coro_th_->Finished()) {
    if (REUSING_CORO) {
      sp_running_coro_th_->status_ = Coroutine::RECYCLED;
      sp_running_coro_th_->func_ = {};
      available_coros_.push_back(sp_running_coro_th_);
    }
    coros_.erase(sp_running_coro_th_);
  }
  sp_running_coro_th_ = sp_old_coro;
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
  bool pause_flag_ ;

  static void* start_poll_loop(void* arg) {
    PollThread* thiz = (PollThread*) arg;
    
    //Put disk I/O thread here for now, but we should move it to clean up code
    
    struct thread_params* args = new struct thread_params;
    args->thread = thiz;
    args->reactor_th = Reactor::GetReactor();

    pthread_t disk_th;
    Pthread_create(&disk_th, nullptr, PollMgr::PollThread::start_disk_loop, args);
    
    thiz->poll_loop();
    delete args;
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
        Coroutine::CreateRun([sp_job]() {sp_job->Work();});
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
    poll_.stop = &stop_flag_ ;
    poll_.pause = &pause_flag_ ;
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
  void pause() { pause_flag_ = true; }
  void resume() { pause_flag_ = false; }
  void update_mode(shared_ptr<Pollable>, int new_mode);

  void add(std::shared_ptr<Job>);
  void remove(std::shared_ptr<Job>);
};

PollMgr::PollMgr(int n_threads /* =... */)
    : n_threads_(n_threads), poll_threads_() {
  verify(n_threads_ > 0);
  poll_threads_ = new PollThread[n_threads_];
  for (int i = 0; i < n_threads_; i++) {
    poll_threads_[i].start(this);
  }
}

PollMgr::~PollMgr() {
  delete[] poll_threads_;
  poll_threads_ = nullptr;
  //Log_debug("rrr::PollMgr: destroyed");
}

void PollMgr::PollThread::poll_loop() {
  while (!stop_flag_) {
    TriggerJob();
    //poll_.Wait();
#ifdef USE_KQUEUE
    poll_.Wait();
#else
    poll_.Wait_One();
    poll_.Wait_Two();
#endif
    verify(Reactor::GetReactor()->ready_events_.empty());
    TriggerJob();
    // after each poll loop, remove uninterested pollables
    pending_remove_l_.lock();
    std::list<shared_ptr<Pollable>> remove_poll(pending_remove_.begin(), pending_remove_.end());
    pending_remove_.clear();
    pending_remove_l_.unlock();

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
    for(int idx = 0; idx < n_threads_; idx++)
    {
        poll_threads_[idx].pause();
    }
}

void PollMgr::resume() {
    for(int idx = 0; idx < n_threads_; idx++)
    {
        poll_threads_[idx].resume();
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
