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
    // Break handle_read into two halves to solve reverse backlog problem
    virtual bool handle_read_one() = 0;
    virtual bool handle_read_two() = 0;
    virtual void handle_write() = 0;
    virtual void handle_error() = 0;
};


class Epoll {
 private:
  std::vector<Pollable*> pending{};
	int zero_count = 0;
	long have_count = 0;
  long no_count = 0;
	int first = 0;
  long total_have_time = 0;
  long total_no_time = 0;
 public:
  volatile bool* pause;
  volatile bool* stop;

  Epoll() {
#ifdef USE_KQUEUE
    poll_fd_ = kqueue();
#else
    poll_fd_ = epoll_create(10);    // arg ignored, any value > 0 will do
#endif
    verify(poll_fd_ != -1);
  }

  std::vector<struct timespec> Wait_One(int& num_ev, bool& slow);
  void Wait_Two();
  void Wait();

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

  ~Epoll() {
    close(poll_fd_);
  }

 private:
  int poll_fd_;
};

} // namespace rrr
