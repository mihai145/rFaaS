
#ifndef __SERVER_FASTEXECUTORS_HPP__
#define __SERVER_FASTEXECUTORS_HPP__

#include <vector>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <sys/types.h>

#include "worker.hpp"
#include <spdlog/spdlog.h>

namespace rdmalib {
  struct RecvBuffer;
}

namespace server {

  struct FastExecutors {

    std::vector<server::Worker> _threads_data;
    std::vector<std::thread> _threads;
    std::vector<pid_t> _workers;
    bool _closing;
    int _numcores;
    int _max_repetitions;
    int _warmup_iters;
    bool _use_multiprocessing;
    int _pin_threads;
    //const ManagerConnection & _mgr_conn;

    FastExecutors(
      std::string client_addr, int port,
      int function_size,
      int numcores,
      int msg_size,
      int recv_buf_size,
      int max_inline_data,
      bool use_multiprocessing,
      int pin_threads,
      const executor::ManagerConnection & mgr_conn
    );
    ~FastExecutors();

    void close();
    void allocate_workers(int, int);
  };

}

#endif

