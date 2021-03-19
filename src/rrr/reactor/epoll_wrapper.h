//
// Created by shuai on 8/22/18.
//
#pragma once

#include "base/all.hpp"
#include <unistd.h>
#include <array>
#include <algorithm>
#include <memory>
#include <vector>

#ifdef __APPLE__
#define USE_KQUEUE
#endif

#ifdef USE_KQUEUE
#include <sys/event.h>
#else
#include <sys/epoll.h>
#endif


namespace rrr {
using std::shared_ptr;

class Pollable: public std::enable_shared_from_this<Pollable> {

public:

    enum {
        READ = 0x1, WRITE = 0x2
    };

    virtual int fd() = 0;
    virtual int poll_mode() = 0;
    virtual size_t content_size() = 0;
    virtual bool handle_read() = 0;
    //virtual void handle_read_one() = 0;
    virtual bool handle_read_two() = 0;
    virtual void handle_write() = 0;
    virtual void handle_error() = 0;
};


class Epoll {
 private:
  std::vector<Pollable*> pending{};
	int zero_count = 0;
	int count = 0;
	int first = 0;
 public:
   volatile bool* pause ;
   volatile bool* stop ;

  Epoll() {
#ifdef USE_KQUEUE
    poll_fd_ = kqueue();
#else
    poll_fd_ = epoll_create(10);    // arg ignored, any value > 0 will do
#endif
    verify(poll_fd_ != -1);
  }

  int Add(shared_ptr<Pollable> poll) {
    auto poll_mode = poll->poll_mode();
    auto fd = poll->fd();
#ifdef USE_KQUEUE
    struct kevent ev;
    if (poll_mode & Pollable::READ) {
      bzero(&ev, sizeof(ev));
      ev.ident = fd;
      ev.flags = EV_ADD;
      ev.filter = EVFILT_READ;
      ev.udata = poll.get();
      verify(kevent(poll_fd_, &ev, 1, nullptr, 0, nullptr) == 0);
    }
    if (poll_mode & Pollable::WRITE) {
      bzero(&ev, sizeof(ev));
      ev.ident = fd;
      ev.flags = EV_ADD;
      ev.filter = EVFILT_WRITE;
      ev.udata = poll.get();
      verify(kevent(poll_fd_, &ev, 1, nullptr, 0, nullptr) == 0);
    }

#else
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));

    ev.data.ptr = poll.get();
    ev.events = EPOLLET | EPOLLIN | EPOLLRDHUP; // EPOLLERR and EPOLLHUP are included by default

    if (poll_mode & Pollable::WRITE) {
        ev.events |= EPOLLOUT;
    }
    verify(epoll_ctl(poll_fd_, EPOLL_CTL_ADD, fd, &ev) == 0);
#endif
    return 0;
  }

  int Remove(shared_ptr<Pollable> poll) {
    auto fd = poll->fd();
#ifdef USE_KQUEUE
    struct kevent ev;

    bzero(&ev, sizeof(ev));
    ev.ident = fd;
    ev.flags = EV_DELETE;
    ev.filter = EVFILT_READ;
    kevent(poll_fd_, &ev, 1, nullptr, 0, nullptr);
    bzero(&ev, sizeof(ev));
    ev.ident = fd;
    ev.flags = EV_DELETE;
    ev.filter = EVFILT_WRITE;
    kevent(poll_fd_, &ev, 1, nullptr, 0, nullptr);

#else
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    epoll_ctl(poll_fd_, EPOLL_CTL_DEL, fd, &ev);
#endif
    return 0;
  }

  int Update(shared_ptr<Pollable> poll, int new_mode, int old_mode) {
    auto fd = poll->fd();
#ifdef USE_KQUEUE
    struct kevent ev;
    if ((new_mode & Pollable::READ) && !(old_mode & Pollable::READ)) {
      // add READ
      bzero(&ev, sizeof(ev));
      ev.ident = fd;
      ev.udata = poll.get();
      ev.flags = EV_ADD;
      ev.filter = EVFILT_READ;
      verify(kevent(poll_fd_, &ev, 1, nullptr, 0, nullptr) == 0);
    }
    if (!(new_mode & Pollable::READ) && (old_mode & Pollable::READ)) {
      // del READ
      bzero(&ev, sizeof(ev));
      ev.ident = fd;
      ev.udata = poll.get();
      ev.flags = EV_DELETE;
      ev.filter = EVFILT_READ;
      verify(kevent(poll_fd_, &ev, 1, nullptr, 0, nullptr) == 0);
    }
    if ((new_mode & Pollable::WRITE) && !(old_mode & Pollable::WRITE)) {
      // add WRITE
      bzero(&ev, sizeof(ev));
      ev.ident = fd;
      ev.udata = poll.get();
      ev.flags = EV_ADD;
      ev.filter = EVFILT_WRITE;
      verify(kevent(poll_fd_, &ev, 1, nullptr, 0, nullptr) == 0);
    }
    if (!(new_mode & Pollable::WRITE) && (old_mode & Pollable::WRITE)) {
      // del WRITE
      bzero(&ev, sizeof(ev));
      ev.ident = fd;
      ev.udata = poll.get();
      ev.flags = EV_DELETE;
      ev.filter = EVFILT_WRITE;
      verify(kevent(poll_fd_, &ev, 1, nullptr, 0, nullptr) == 0);
    }
#else
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));

    ev.data.ptr = poll.get();
    ev.events = EPOLLET | EPOLLRDHUP;
    if (new_mode & Pollable::READ) {
        ev.events |= EPOLLIN;
    }
    if (new_mode & Pollable::WRITE) {
        ev.events |= EPOLLOUT;
    }
    verify(epoll_ctl(poll_fd_, EPOLL_CTL_MOD, fd, &ev) == 0);
#endif
    return 0;
  }


	std::vector<struct timespec> Wait_One(int& num_ev, bool& slow) {
    const int max_nev = 100;
		bool found = false;
		std::vector<struct timespec> result{};
#ifdef USE_KQUEUE
    struct kevent evlist[max_nev];
    struct timespec timeout;
    timeout.tv_sec = 0;
    timeout.tv_nsec = 50 * 1000 * 1000; // 0.05 sec

    int nev = kevent(poll_fd_, nullptr, 0, evlist, max_nev, &timeout);

    for (int i = 0; i < nev; i++){
			found = true;
      Pollable* poll = (Pollable *) evlist[i].udata;
      if (evlist[i].filter & EVFILT_READ){
        poll->handle_read();
        pending.push_back(poll);
      }
      if (evlist[i].filter & EVFILT_WRITE){
        poll->handle_write();
      }
      if (evlist[i].flags & EV_EOF){
        poll->handle_error();
      }
    }

#else
    struct epoll_event evlist[max_nev];
    int timeout = 1; // milli, 0.001 sec
//    int timeout = 0; // busy loop
    
		struct timespec begin_cpu, begin, begin_wait, end_wait, begin_wait_cpu, end_wait_cpu;
		clock_gettime(CLOCK_MONOTONIC_RAW, &begin);
		clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &begin_cpu);
		clock_gettime(CLOCK_MONOTONIC_RAW, &begin_wait);
		clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &begin_wait_cpu);
    int nev = epoll_wait(poll_fd_, evlist, max_nev, timeout);
		
		clock_gettime(CLOCK_MONOTONIC_RAW, &end_wait);
		clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &end_wait_cpu);
		num_ev = nev;
    
    for (int i = 0; i < nev; i++) {
			found = true;
      Pollable* poll = (Pollable *) evlist[i].data.ptr;
      verify(poll != nullptr);
      if (evlist[i].events & EPOLLIN) {
					bool push = poll->handle_read();
          if(push) pending.push_back(poll);
      }
      if (evlist[i].events & EPOLLOUT) {
          poll->handle_write();
      }

      // handle error after handle IO, so that we can at least process something
      if (evlist[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
          poll->handle_error();
      }
    }
		
		count++;
		if (found) {
			result.push_back(begin);
			result.push_back(begin_cpu);
		}
		result.push_back(begin_wait);
		result.push_back(end_wait);
		result.push_back(begin_wait_cpu);
		result.push_back(end_wait_cpu);
    
#endif
		return result;
  }

  void Wait_Two() {
		struct timespec begin2, begin2_cpu, end2, end2_cpu;
		/*clock_gettime(CLOCK_MONOTONIC, &begin2);		
		clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &begin2_cpu);*/
    
    for(auto it = pending.begin(); it != pending.end();){
      Pollable* poll = *it;
      bool done = poll->handle_read_two();
      if(done){
        it = pending.erase(it);
      } else{
        it++;
      }
    }
		/*if (pending.size() != 0) {
			clock_gettime(CLOCK_MONOTONIC, &end2);
			clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &end2_cpu);
			long total_cpu2 = (end2_cpu.tv_sec - begin2_cpu.tv_sec)*1000000000 + (end2_cpu.tv_nsec - begin2_cpu.tv_nsec);
			long total_time2 = (end2.tv_sec - begin2.tv_sec)*1000000000 + (end2.tv_nsec - begin2.tv_nsec);
			double util2 = (double) total_cpu2/total_time2;
			Log_info("elapsed CPU time (client read2): %f", util2);
		}*/
  }

  void Wait() {
    const int max_nev = 100;
#ifdef USE_KQUEUE
    struct kevent evlist[max_nev];
    struct timespec timeout;
    timeout.tv_sec = 0;
    timeout.tv_nsec = 50 * 1000 * 1000; // 0.05 sec

		int nev = kevent(poll_fd_, nullptr, 0, evlist, max_nev, &timeout);

		for (int i = 0; i < nev; i++) {
      Pollable* poll = (Pollable*) evlist[i].udata;
      verify(poll != nullptr);

      if (evlist[i].filter == EVFILT_READ) {
        poll->handle_read();
      }
      if (evlist[i].filter == EVFILT_WRITE) {
        poll->handle_write();
      }

      // handle error after handle IO, so that we can at least process something
      if (evlist[i].flags & EV_EOF) {
        poll->handle_error();
      }
    }

#else
    struct epoll_event evlist[max_nev];
    int timeout = 1; // milli, 0.001 sec
//    int timeout = 0; // busy loop
    int nev = epoll_wait(poll_fd_, evlist, max_nev, timeout);

    if(nev != 0){
      /*Log_info("stuck here: %d", nev);
      std::vector<size_t> lengths(nev);
      for (int i = 0; i < nev; i++) {
        Pollable* poll = (Pollable *) evlist[i].data.ptr;
        lengths[i] = poll->content_size();
      }

      std::vector<size_t> args(nev);
      std::iota(args.begin(), args.end(), 0);
      std::sort(args.begin(), args.end(), [&lengths](int i1, int i2) { return lengths[i1] < lengths[i2];} );
      Log_info("not stuck here");*/
    }
    
    
    for (int i = 0; i < nev; i++) {
      Pollable* poll = (Pollable *) evlist[i].data.ptr;
      verify(poll != nullptr);

      if (evlist[i].events & EPOLLIN) {
          poll->handle_read();
      }
      if (evlist[i].events & EPOLLOUT) {
          poll->handle_write();
      }

      // handle error after handle IO, so that we can at least process something
      if (evlist[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
          poll->handle_error();
      }
    }
#endif
  }

  ~Epoll() {
    close(poll_fd_);
  }

 private:
  int poll_fd_;
};

} // namespace rrr
