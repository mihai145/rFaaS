#include <rfaas/rfaas.hpp>
#include <rdmalib/rdmalib.hpp>
#include <rdmalib/functions.hpp>
#include <fstream>
#include <utility>

typedef rfaas::device_data *device_data;
typedef rfaas::client *rfaas_client;
typedef rfaas::executor *rfaas_executor;

struct rfaas_inputs_outputs
{
    std::vector<rdmalib::Buffer<char>> inputs;
    std::vector<rdmalib::Buffer<char>> outputs;
};
typedef struct rfaas_inputs_outputs *rfaas_inputs_outputs_handle;

extern "C"
{
    void deserialize_devices(const char *device_database_fname)
    {
        std::ifstream in_dev(device_database_fname);
        rfaas::devices::deserialize(in_dev);
        in_dev.close();
    }

    device_data get_device_data(const char *device_name)
    {
        rfaas::device_data *dev = rfaas::devices::instance().device(device_name);
        return dev;
    }

    rfaas_client initialize_rfaas_client(
        const char *resource_manager_address,
        int resource_manager_port,
        device_data device)
    {
        rfaas::client *client = new rfaas::client(
            resource_manager_address, resource_manager_port, *device);

        if (!client->connect())
        {
            delete client;
            return NULL;
        }

        return client;
    }

    rfaas_executor lease(
        rfaas_client client, int numcores, int memory, device_data device)
    {
        auto leased_executor = client->lease(numcores, memory, *device);
        if (!leased_executor.has_value())
        {
            return NULL;
        }

        rfaas_executor executor = new rfaas::executor(std::move(leased_executor.value()));
        return executor;
    }

    bool allocate(rfaas_executor executor, const char *flib, int input_size, int hot_timeout)
    {
        return executor->allocate(flib, input_size, hot_timeout);
    }

    rfaas_inputs_outputs_handle create_rfaas_inputs_outputs()
    {
        return new rfaas_inputs_outputs;
    }

    void destroy_rfaas_inputs_outputs(rfaas_inputs_outputs_handle handle)
    {
        delete handle;
    }

    uint8_t *add_input(rfaas_executor executor, rfaas_inputs_outputs_handle handle, int input_size)
    {
        auto &inputs = handle->inputs;
        inputs.emplace_back(input_size, rdmalib::functions::Submission::DATA_HEADER_SIZE);
        inputs.back().register_memory(executor->_state.pd(), IBV_ACCESS_LOCAL_WRITE);
        return (uint8_t *)inputs.back().data();
    }

    uint8_t *add_output(rfaas_executor executor, rfaas_inputs_outputs_handle handle, int output_size)
    {
        auto &outputs = handle->outputs;
        outputs.emplace_back(output_size);
        outputs.back().register_memory(executor->_state.pd(), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
        return (uint8_t *)outputs.back().data();
    }

    bool execute(rfaas_executor executor, const char *fname, rfaas_inputs_outputs_handle handle)
    {
        return executor->execute(fname, handle->inputs, handle->outputs);
    }

    void destroy_rfaas_executor(rfaas_executor executor)
    {
        if (executor != NULL)
        {
            executor->deallocate();
            delete executor;
        }
    }

    void destroy_rfaas_client(rfaas_client client)
    {
        if (client != NULL)
        {
            client->disconnect();
            delete client;
        }
    }
}
