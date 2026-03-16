
#include <iostream>
#include <unistd.h>

#include <spdlog/spdlog.h>

#include "common.hpp"
#include "worker.hpp"
#include "worker_opts.hpp"

int main(int argc, char **argv)
{
  auto opts = worker::opts(argc, argv);
  spdlog::info(
      "Worker process with id {} started. "
      "Options received from executor: expecting function size {}, function payloads {},"
      " receive WCs buffer size {}, max inline data {}, hot polling timeout {}, iterations {},"
      " manager runs at {}:{}, its secret is {}, the accounting buffer is at {} with rkey {}",
      opts.id, opts.functions_size, opts.buf_size, opts.recv_buffer_size, opts.max_inline_data,
      opts.timeout, opts.iterations, opts.mgr_conn_addr, opts.mgr_conn_port, opts.mgr_conn_secret,
      opts.mgr_conn_r_addr, opts.mgr_conn_r_key);

  executor::ManagerConnection mgr{
      opts.mgr_conn_addr,
      opts.mgr_conn_port,
      opts.mgr_conn_secret,
      opts.mgr_conn_r_addr,
      opts.mgr_conn_r_key};

  server::Worker worker(opts.addr, opts.port, opts.id, opts.functions_size, opts.buf_size, opts.recv_buffer_size, opts.max_inline_data, mgr);
  worker.max_repetitions = opts.iterations;

  worker.thread_work(opts.timeout);

  spdlog::info("Worker process with id {} ended", opts.id);

  return 0;
}
