#include "rdmalib/rdmalib.hpp"

#include "worker.hpp"

namespace server {
  Accounting::timepoint_t Worker::work(int invoc_id, int func_id, bool solicited, uint32_t in_size)
  {
    // FIXME: load func ptr
    rdmalib::functions::Submission* header = reinterpret_cast<rdmalib::functions::Submission*>(rcv.ptr());
    auto ptr = _functions.function(func_id);

    SPDLOG_DEBUG("Thread {} begins work! Executing function {} with size {}, invoc id {}, solicited reply? {}",
      id, _functions._names[func_id], in_size, invoc_id, solicited
    );
    auto start = std::chrono::high_resolution_clock::now();
    // Data to ignore header passed in the buffer
    uint32_t out_size = (*ptr)(rcv.data(), in_size, send.ptr());
    SPDLOG_DEBUG("Thread {} finished work!", id);

    // Send back: the value of immediate write
    // first 16 bytes - invocation id
    // second 16 bytes - return value (0 on no error)
    conn->post_write(
      send.sge(out_size, 0),
      {header->r_address, header->r_key},
      (invoc_id << 16) | 0,
      out_size <= max_inline_data,
      solicited
    );
    auto end = std::chrono::high_resolution_clock::now();
    _accounting.update_execution_time(start, end);
    _accounting.send_updated_execution(_mgr_connection, _accounting_buf, _mgr_conn);
    //int cpu = sched_getcpu();
    //spdlog::info("Execution + sent took {} us on {} CPU", std::chrono::duration_cast<std::chrono::microseconds>(end-start).count(), cpu);
    return end;
  }

  void Worker::hot(uint32_t timeout)
  {
    //rdmalib::Benchmarker<1> server_processing_times{max_repetitions};
    SPDLOG_DEBUG("Thread {} Begins hot polling", id);

    auto start = std::chrono::high_resolution_clock::now();
    int i = 0;
    while(repetitions < max_repetitions) {

      // if we block, we never handle the interruption
      auto wcs = this->conn->receive_wcs().poll();
      if(std::get<1>(wcs)) {
        for(int i = 0; i < std::get<1>(wcs); ++i) {

          //server_processing_times.start();
          ibv_wc* wc = &std::get<0>(wcs)[i];
          if(wc->status) {
            spdlog::error("Failed work completion! Reason: {}", ibv_wc_status_str(wc->status));
            continue;
          }
          int info = ntohl(wc->imm_data);
          int func_id = info & invocation_mask;
          int invoc_id = info >> 16;
          bool solicited = info & solicited_mask;
          SPDLOG_DEBUG(
            "Thread {} Invoc id {} Execute func {} Repetition {}",
            id, invoc_id, func_id, repetitions
          );

          // Measure hot polling time until we started execution
          auto now = std::chrono::high_resolution_clock::now();
          auto func_end = work(invoc_id, func_id, solicited,
              wc->byte_len - rdmalib::functions::Submission::DATA_HEADER_SIZE
          );
          _accounting.update_polling_time(start, now);
          i = 0;
          start = func_end;

          //sum += server_processing_times.end();
          conn->poll_wc(rdmalib::QueueType::SEND, true);
          repetitions += 1;
        }
        this->conn->receive_wcs().refill();
      }
      ++i;

      // FIXME: adjust period to the timeout
      if(i == HOT_POLLING_VERIFICATION_PERIOD) {
        auto now = std::chrono::high_resolution_clock::now();
        auto time_passed = _accounting.update_polling_time(start, now);
        _accounting.send_updated_polling(_mgr_connection, _accounting_buf, _mgr_conn);
        start = now;

        if(_polling_state != PollingState::HOT_ALWAYS && time_passed >= timeout) {
          _polling_state = PollingState::WARM;
          // FIXME: can we miss an event here?
          conn->notify_events();
          SPDLOG_DEBUG("Switching to warm polling after {} us with no invocations", time_passed);
          return;
        }
        i = 0;
      }
    }
  }

  void Worker::warm()
  {
    //rdmalib::Benchmarker<1> server_processing_times{max_repetitions};
    // FIXME: this should be automatic
    SPDLOG_DEBUG("Thread {} Begins warm polling", id);

    while(repetitions < max_repetitions) {

      // if we block, we never handle the interruption
      auto wcs = this->conn->receive_wcs().poll();
      if(std::get<1>(wcs)) {
        for(int i = 0; i < std::get<1>(wcs); ++i) {

          //server_processing_times.start();
          ibv_wc* wc = &std::get<0>(wcs)[i];
          if(wc->status) {
            spdlog::error("Failed work completion! Reason: {}", ibv_wc_status_str(wc->status));
            continue;
          }
          int info = ntohl(wc->imm_data);
          int func_id = info & invocation_mask;
          bool solicited = info & solicited_mask;
          int invoc_id = info >> 16;
          SPDLOG_DEBUG(
            "Thread {} Invoc id {} Execute func {} Repetition {}",
            id, invoc_id, func_id, repetitions
          );

          work(invoc_id, func_id, solicited, wc->byte_len - rdmalib::functions::Submission::DATA_HEADER_SIZE);

          //sum += server_processing_times.end();
          conn->poll_wc(rdmalib::QueueType::SEND, true);
          repetitions += 1;
        }
        this->conn->receive_wcs().refill();
        if(_polling_state != PollingState::WARM_ALWAYS) {
          SPDLOG_DEBUG("Switching to hot polling after invocation!");
          _polling_state = PollingState::HOT;
          return;
        }
      }

      // Do waiting after a single polling - avoid missing an events that
      // arrived before we called notify_events
      if(repetitions < max_repetitions) {
        auto cq = conn->wait_events();
        conn->ack_events(cq, 1);
        conn->notify_events();
      }
    }
    SPDLOG_DEBUG("Thread {} Stopped warm polling", id);
  }

  void Worker::thread_work(int timeout)
  {
    rdmalib::RDMAActive mgr_connection(_mgr_conn.addr, _mgr_conn.port, _recv_buffer_size, max_inline_data);
    mgr_connection.allocate();
    this->_mgr_connection = &mgr_connection.connection();
    _accounting_buf.register_memory(mgr_connection.pd(), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
    if(!mgr_connection.connect(_mgr_conn.secret))
      return;
    spdlog::info("Thread {} Established connection to the manager!", id);

    rdmalib::RDMAActive active(addr, port, _recv_buffer_size, max_inline_data);
    rdmalib::Buffer<char> func_buffer(_functions.memory(), _functions.size());

    active.allocate();
    this->conn = &active.connection();
    // Receive function data from the client - this WC must be posted first
    // We do it before connection to ensure that client does not start sending before us
    func_buffer.register_memory(active.pd(), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    this->conn->post_recv(func_buffer);

    // Request notification before connecting - avoid missing a WC!
    // Do it only when starting from a warm directly
    if(timeout == -1) {
      _polling_state = PollingState::HOT_ALWAYS;
    } else if(timeout == 0) {
      _polling_state = PollingState::WARM_ALWAYS;
    } else {
      _polling_state = PollingState::HOT;
    }
    if(_polling_state == PollingState::WARM_ALWAYS || _polling_state == PollingState::WARM)
      conn->notify_events();

    if(!active.connect())
      return;

    // Now generic receives for function invocations
    send.register_memory(active.pd(), IBV_ACCESS_LOCAL_WRITE);
    rcv.register_memory(active.pd(), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);

    spdlog::info("Thread {} Established connection to client!", id);

    // Send to the client information about thread buffer
    rdmalib::Buffer<rdmalib::BufferInformation> buf(1);
    buf.register_memory(active.pd(), IBV_ACCESS_LOCAL_WRITE);
    buf.data()[0].r_addr = rcv.address();
    buf.data()[0].r_key = rcv.rkey();
    SPDLOG_DEBUG("Thread {} Sends buffer details to client!", id);
    this->conn->post_send(buf, 0, buf.size() <= max_inline_data);
    this->conn->poll_wc(rdmalib::QueueType::SEND, true, 1);
    SPDLOG_DEBUG("Thread {} Sent buffer details to client!", id);

    // We should have received functions data - just one message
    this->conn->poll_wc(rdmalib::QueueType::RECV, true, 1);
    _functions.process_library();

    this->conn->receive_wcs().refill();
    spdlog::info("Thread {} begins work with timeout {}", id, timeout);

    // FIXME: catch interrupt handler here
    while(repetitions < max_repetitions) {
      if(_polling_state == PollingState::HOT || _polling_state == PollingState::HOT_ALWAYS)
        hot(timeout);
      else
        warm();
    }

    // Submit final accounting information
    _accounting.send_updated_execution(_mgr_connection, _accounting_buf, _mgr_conn, true, false);
    _accounting.send_updated_polling(_mgr_connection, _accounting_buf, _mgr_conn, true, false);
    mgr_connection.connection().poll_wc(rdmalib::QueueType::SEND, true, 2);
    spdlog::info(
      "Thread {} finished work, spent {} ns hot polling and {} ns computation, {} executions.",
      id, _accounting.total_hot_polling_time , _accounting.total_execution_time, repetitions
    );
    // FIXME: revert after manager starts to detect disconnection events
    //mgr_connection.disconnect();
  }
}