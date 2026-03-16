
#include <chrono>
#include <atomic>
#include <ostream>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>

#include <spdlog/spdlog.h>
#include <spdlog/common.h>

#include <rdmalib/benchmarker.hpp>
#include <rdmalib/util.hpp>
#include "rdmalib/buffer.hpp"
#include "rdmalib/connection.hpp"
#include "rdmalib/functions.hpp"
#include "rdmalib/rdmalib.hpp"

#include <rfaas/allocation.hpp>

#include "server.hpp"
#include "fast_executor.hpp"

#include <sched.h>

namespace server {

  FastExecutors::FastExecutors(std::string client_addr, int port,
      int func_size,
      int numcores,
      int msg_size,
      int recv_buf_size,
      int max_inline_data,
      bool use_multiprocessing,
      int pin_threads,
      const executor::ManagerConnection & mgr_conn
  ):
    _closing(false),
    _function_size(func_size),
    _msg_size(msg_size),
    _recv_buf_size(recv_buf_size),
    _max_inline_data(max_inline_data),
    _numcores(numcores),
    _max_repetitions(0),
    _use_multiprocessing(use_multiprocessing),
    _pin_threads(pin_threads)
    //_mgr_conn(mgr_conn)
  {
    // Reserve place to ensure that no reallocations happen
    _workers_data.reserve(numcores);
    for(int i = 0; i < numcores; ++i)
      _workers_data.emplace_back(
        client_addr, port, i, func_size, msg_size,
        recv_buf_size, max_inline_data, mgr_conn
      );
  }

  FastExecutors::~FastExecutors()
  {
    spdlog::info("FastExecutor is closing workers...");
    close();
  }

  void FastExecutors::close()
  {
    if(_closing)
      return;
    // FIXME: this should be only for 'warm'
    //{
    //  std::lock_guard<std::mutex> g(m);
    //  _closing = true;
    //  // wake threads, letting them exit
    //  wakeup();
    //}
    // make sure we join before destructing
    SPDLOG_DEBUG("Wait on {} threads", _threads.size());
    for(auto & thread : _threads)
      // Might have been closed earlier
      if(thread.joinable())
        thread.join();
    SPDLOG_DEBUG("Finished wait on {} threads", _threads.size());

    SPDLOG_DEBUG("Wait on {} processes", _workers.size());
    for (int pid : _workers) {
      int status;
      if (waitpid(pid, &status, 0) < 0) {
        spdlog::info("Failed waitpid on {}", pid);
      } else {
        spdlog::info("Worker {} exited with status {}", pid, status);
      }
    }
    SPDLOG_DEBUG("Finished wait on {} processes", _workers.size());

    for(auto & thread : _workers_data)
      spdlog::info("Thread {} Repetitions {} Avg time {} ms",
        thread.id,
        thread.repetitions,
        static_cast<double>(thread._accounting.total_execution_time) / thread.repetitions / 1000.0
      );

    _closing = true;
  }

  void FastExecutors::allocate_workers(int timeout, int iterations)
  {
    if (_use_multiprocessing) {
      for(int i = 0; i < _numcores; ++i) {
        pid_t pid = fork();
        if(pid < 0) {
          spdlog::error("Fork failed for {}-th worker! {}", i, pid);
        }

        if (pid == 0) {
          const char * argv[] = {
            "worker",
            "--addr", _workers_data[i].addr.c_str(),
            "--port", std::to_string(_workers_data[i].port).c_str(),
            "--id", std::to_string(_workers_data[i].id).c_str(),
            "--functions_size", std::to_string(_function_size).c_str(),
            "--buf_size", std::to_string(_msg_size).c_str(),
            "--recv_buffer_size", std::to_string(_recv_buf_size).c_str(),
            "--max_inline_data", std::to_string(_max_inline_data).c_str(),
            "--mgr_conn_addr", _workers_data[i]._mgr_conn.addr.c_str(),
            "--mgr_conn_port", std::to_string(_workers_data[i]._mgr_conn.port).c_str(),
            "--mgr_conn_secret", std::to_string(_workers_data[i]._mgr_conn.secret).c_str(),
            "--mgr_conn_r_addr", std::to_string(_workers_data[i]._mgr_conn.r_addr).c_str(),
            "--mgr_conn_r_key", std::to_string(_workers_data[i]._mgr_conn.r_key).c_str(),
            "--timeout", std::to_string(timeout).c_str(),
            nullptr
          };
          int ret = execvp(argv[0], const_cast<char**>(&argv[0]));
          if(ret == -1) {
            spdlog::error("Worker process failed {}, reason {}", errno, strerror(errno));
            exit(1);
          }
        }

        spdlog::info("Forked {}-th worker, pid {}", i, pid);
        _workers.push_back(pid);
      }

      return;
    }

    // multithreading
    int pin_threads = _pin_threads;
    for(int i = 0; i < _numcores; ++i) {
      _workers_data[i].max_repetitions = iterations;
      _threads.emplace_back(
        &Worker::thread_work,
        &_workers_data[i],
        timeout
      );
      // FIXME: make sure that native handle is actually from pthreads
      if(pin_threads != -1) {
        spdlog::info("Pin thread to core {}", pin_threads);
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(pin_threads++, &cpuset);
        rdmalib::impl::expect_zero(pthread_setaffinity_np(
          _threads[i].native_handle(),
          sizeof(cpu_set_t), &cpuset
        ));
      }
    }
  }

  //void FastExecutors::serial_thread_poll_func(int)
  //{
  //  uint64_t sum = 0;
  //  int repetitions = 0;
  //  int total_iters = _max_repetitions + _warmup_iters;
  //  constexpr int cores_mask = 0x3F;
  //  rdmalib::Benchmarker<2> server_processing_times{total_iters};

  //  // FIXME: disable signal handling
  //  //while(!server::SignalHandler::closing && repetitions < total_iters) {
  //  while(repetitions < total_iters) {

  //    // if we block, we never handle the interruption
  //    auto wcs = _wc_buffer->poll();
  //    if(std::get<1>(wcs)) {
  //      for(int i = 0; i < std::get<1>(wcs); ++i) {

  //        server_processing_times.start();
  //        ibv_wc* wc = &std::get<0>(wcs)[i];
  //        if(wc->status) {
  //          spdlog::error("Failed work completion! Reason: {}", ibv_wc_status_str(wc->status));
  //          continue;
  //        }
  //        int info = ntohl(wc->imm_data);
  //        int func_id = info >> 6;
  //        int core = info & cores_mask;
  //        // FIXME: verify function data - valid ID
  //        SPDLOG_DEBUG("Execute func {} at core {}", func_id, core);

  //        SPDLOG_DEBUG("Wake-up fast thread {}", core);
  //        work(core, func_id);

  //        // clean send queue
  //        // FIXME: this should be option - in reality, we don't want to wait for the transfer to end
  //        sum += server_processing_times.end();
  //        _conn->poll_wc(rdmalib::QueueType::SEND, true);
  //        repetitions += 1;
  //      }
  //      _wc_buffer->refill();
  //    }
  //  }
  //  server_processing_times.export_csv("server.csv", {"process", "send"});

  //  _time_sum.fetch_add(sum / 1000.0);
  //  _repetitions.fetch_add(repetitions);
  //}

  //void FastExecutors::thread_poll_func(int id)
  //{
  //  uint64_t sum = 0;
  //  int total_iters = _max_repetitions + _warmup_iters;
  //  constexpr int cores_mask = 0x3F;
  //  rdmalib::Benchmarker<1> server_processing_times{total_iters};

  //  int64_t _poller_empty = -1;
  //  bool i_am_poller = false;
  //  //while(_iterations < total_iters) {
  //  while(true) {

  //    int64_t cur_poller = _cur_poller.load();
  //    if(cur_poller == _poller_empty) {
  //      i_am_poller = _cur_poller.compare_exchange_strong(
  //          _poller_empty, id, std::memory_order_release, std::memory_order_relaxed
  //      );
  //      SPDLOG_DEBUG("Thread {} Attempted to become a poller, result {}", id, i_am_poller);
  //    } else {
  //      i_am_poller = cur_poller == id;
  //    }

  //    // Now wait for assignment or perform polling
  //    if(i_am_poller) {
  //      SPDLOG_DEBUG("Thread {} Performs polling", id);
  //      while(i_am_poller && _iterations < total_iters) {
  //        // if we block, we never handle the interruption
  //        auto wcs = _wc_buffer->poll();
  //        if(std::get<1>(wcs)) {
  //          SPDLOG_DEBUG("Thread {} Polled {} wcs", id, std::get<1>(wcs));

  //          int work_myself = 0;
  //          for(int i = 0; i < std::get<1>(wcs); ++i) {

  //            server_processing_times.start();
  //            ibv_wc* wc = &std::get<0>(wcs)[i];
  //            if(wc->status) {
  //              spdlog::error("Failed work completion! Reason: {}", ibv_wc_status_str(wc->status));
  //              continue;
  //            }
  //            int info = ntohl(wc->imm_data);
  //            int func_id = info >> 6;
  //            int core = info & cores_mask;
  //            // FIXME: verify function data - valid ID
  //            SPDLOG_DEBUG("Execute func {} at core {}", func_id, core);

  //            if(core != id) {
  //              if(this->_thread_status[core].load() == 0) {
  //                SPDLOG_DEBUG("Wake-up fast thread {} by thread {}", core, id);
  //                // No need to release - we only pass an atomic to other thread
  //                this->_thread_status[core].store(func_id);
  //                _iterations += 1;
  //              }
  //              // In benchmarking, this should only happen in debug mode 
  //              else {
  //                SPDLOG_DEBUG("Thread {} busy, send error to client", core);
  //                // Send an error message "1" - thread busy
  //                char* data = static_cast<char*>(_rcv[core].ptr());
  //                uint64_t r_addr = *reinterpret_cast<uint64_t*>(data);
  //                uint32_t r_key = *reinterpret_cast<uint32_t*>(data + 8);
  //                _conn->post_write(
  //                  {},
  //                  {r_addr, r_key},
  //                  (core & cores_mask) | (1 << 6)
  //                );
  //              }
  //              sum += server_processing_times.end();
  //            } else {
  //              work_myself = func_id;
  //            }
  //          }
  //          if(work_myself) {
  //            SPDLOG_DEBUG("Wake-up myself {}", id);
  //            _iterations += 1;
  //            // Release is needed to make sure that `iterations` have been updated
  //            _cur_poller.store(_poller_empty, std::memory_order_release);
  //            i_am_poller = false;
  //            this->_thread_status[id].store(1);
  //            work(id, work_myself);
  //            //_conn->poll_wc(rdmalib::QueueType::SEND, false);
  //          }
  //          SPDLOG_DEBUG("Thread {} Refill");
  //          _wc_buffer->refill();
  //          _conn->poll_wc(rdmalib::QueueType::SEND, false);
  //          SPDLOG_DEBUG("Thread {} Refilled");
  //        }
  //      }
  //      // we finished iterations
  //      if(i_am_poller) {
  //        // Wait for others to finish
  //        int64_t expected = 0;
  //        for(int i = 0; i < _numcores; ++i) {
  //          while(!_thread_status[i].compare_exchange_strong(
  //            expected, -1, std::memory_order_release, std::memory_order_relaxed
  //          ));
  //        }
  //        break;
  //      }
  //    } else {
  //      SPDLOG_DEBUG("Thread {} Waits for work", id);
  //      int64_t status = 0;
  //      do {
  //        status = this->_thread_status[id].load();
  //        cur_poller = _cur_poller.load();
  //      } while(status == 0 && cur_poller != -1);
  //      if(status > 0) {
  //        SPDLOG_DEBUG("Thread {} Got work!", id);
  //        work(id, status);
  //        // clean send queue
  //        // FIXME: this should be option - in reality, we don't want to wait for the transfer to end
  //        //_conn->poll_wc(rdmalib::QueueType::SEND, false);
  //        SPDLOG_DEBUG("Thread {} Finished work!", id);
  //      } else {
  //        break;
  //      }
  //    }
  //  }
  //  server_processing_times.export_csv("server.csv", {"process"});

  //  _time_sum.fetch_add(sum / 1000.0);
  //  _repetitions.fetch_add(_iterations);
  //  spdlog::info("Thread {} Finished!", id);
  //}

  //void FastExecutors::cv_thread_func(int id)
  //{
  //  int sum = 0;
  //  timeval end;
  //  std::unique_lock<std::mutex> lk(m);
  //  SPDLOG_DEBUG("Thread {} created!", id);
  //  while(true) {

  //    SPDLOG_DEBUG("Thread {} goes to sleep! Closing {} ptr {}", id, _closing, _threads_status[id].func != nullptr);
  //    if(!lk.owns_lock())
  //     lk.lock();
  //    _cv.wait(lk, [this, id](){
  //        return _threads_status[id].func || _closing;
  //    });
  //    lk.unlock();
  //    SPDLOG_DEBUG("Thread {} wakes up! {}", id, _closing);

  //    // We don't exit unless there's no more work to do.
  //    if(_closing && !_threads_status[id].func) {
  //      SPDLOG_DEBUG("Thread {} exits!", id);
  //      break;
  //    }

  //    work(id, 0);
  //    gettimeofday(&end, nullptr);
  //    int usec = (end.tv_sec - _start_timestamps[id].tv_sec) * 1000000 + (end.tv_usec - _start_timestamps[id].tv_usec);
  //    sum += usec;
  //    SPDLOG_DEBUG("Thread {} loops again!", id);
  //  } 
  //  SPDLOG_DEBUG("Thread {} exits!", id);
  //  _time_sum.fetch_add(sum);
  //}
}

