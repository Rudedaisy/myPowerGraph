/*AUTOMATICALLY GENERATED CODE*/

#include <simgrid/s4u.hpp>

XBT_LOG_NEW_DEFAULT_CATEGORY(s4u_app_masterworker, "Messages specific for this example");

#define FLIT_SIZE 64
#define SNOOP_SIZE 64
#define dummy_cost 10

void checkAndRec(simgrid::s4u::Mailbox* mailbox) {
  if (!mailbox->empty()) {
    double* signal = static_cast<double*>(mailbox->get());
    delete signal;
  }
}

void checkAndSend(simgrid::s4u::Mailbox* mailbox) {
  if (!mailbox->empty()) {
    mailbox->put(new double(dummy_cost), FLIT_SIZE);
  }
}


static void host(std::vector<std::string> args) {
  xbt_assert(args.size() == 1, "The host expects no argument.");
  simgrid::s4u::Mailbox* host_data_mailbox     = simgrid::s4u::Mailbox::by_name("host_data");
  simgrid::s4u::Mailbox* remote_data_mailbox   = simgrid::s4u::Mailbox::by_name("remote_data");
  simgrid::s4u::Mailbox* device_data_mailbox   = simgrid::s4u::Mailbox::by_name("device_data");
  simgrid::s4u::Mailbox* host_sig_mailbox      = simgrid::s4u::Mailbox::by_name("host_sig");
  simgrid::s4u::Mailbox* device_sig_mailbox    = simgrid::s4u::Mailbox::by_name("device_sig");

double* data = NULL;
double* signal = NULL;

  XBT_INFO("host send invalidates");
  device_sig_mailbox->put(new double(dummy_cost), SNOOP_SIZE);
  XBT_INFO("host receive 66 bytes of data from remote");
  data = static_cast<double*>(remote_data_mailbox->get());
  delete data;
  XBT_INFO("host receive 66 bytes of data from remote");
  data = static_cast<double*>(remote_data_mailbox->get());
  delete data;

  signal = static_cast<double*>(host_sig_mailbox->get());
  delete signal;
  host_data_mailbox->put(new double(dummy_cost), 66);
   XBT_INFO("Host exiting.");
}

static void remote(std::vector<std::string> args) {
  xbt_assert(args.size() == 1, "The remote expects no argument.");
  simgrid::s4u::Mailbox* host_data_mailbox     = simgrid::s4u::Mailbox::by_name("host_data");
  simgrid::s4u::Mailbox* remote_data_mailbox   = simgrid::s4u::Mailbox::by_name("remote_data");
  simgrid::s4u::Mailbox* device_data_mailbox   = simgrid::s4u::Mailbox::by_name("device_data");
  simgrid::s4u::Mailbox* host_sig_mailbox      = simgrid::s4u::Mailbox::by_name("host_sig");
  simgrid::s4u::Mailbox* device_sig_mailbox    = simgrid::s4u::Mailbox::by_name("device_sig");

double* data = NULL;
double* signal = NULL;

  remote_data_mailbox->put(new double(dummy_cost), 66);
  remote_data_mailbox->put(new double(dummy_cost), 66);
  remote_data_mailbox->put(new double(dummy_cost), 66);
  XBT_INFO("Remote exiting.");
}

static void device(std::vector<std::string> args) {
  xbt_assert(args.size() == 1, "The device expects no argument.");
  simgrid::s4u::Mailbox* host_data_mailbox     = simgrid::s4u::Mailbox::by_name("host_data");
  simgrid::s4u::Mailbox* remote_data_mailbox   = simgrid::s4u::Mailbox::by_name("remote_data");
  simgrid::s4u::Mailbox* device_data_mailbox   = simgrid::s4u::Mailbox::by_name("device_data");
  simgrid::s4u::Mailbox* host_sig_mailbox      = simgrid::s4u::Mailbox::by_name("host_sig");
  simgrid::s4u::Mailbox* device_sig_mailbox    = simgrid::s4u::Mailbox::by_name("device_sig");

double* data = NULL;
double* signal = NULL;

  signal = static_cast<double*>(device_sig_mailbox->get());
  delete signal;
  signal = static_cast<double*>(device_sig_mailbox->get());
  delete signal;
  XBT_INFO("device send invalidates");
  host_sig_mailbox->put(new double(dummy_cost), SNOOP_SIZE);
  XBT_INFO("device receive 66 bytes of data from remote");
  data = static_cast<double*>(remote_data_mailbox->get());
  delete data;
  XBT_INFO("device receive 66 bytes of data from host");
  data = static_cast<double*>(host_data_mailbox->get());
  delete data;
  XBT_INFO("device send invalidates");
  host_sig_mailbox->put(new double(dummy_cost), SNOOP_SIZE);
  XBT_INFO("Device exiting.");
}

int main(int argc, char* argv[]) {
  simgrid::s4u::Engine e(&argc, argv);
  xbt_assert(argc > 2, "Usage: %s platform_file deployment_file", argv[0]);

  /* Register the functions representing the actors */
  e.register_function("host", &host);
  e.register_function("remote", &remote);
  e.register_function("device", &device);

  /* Load the platform description and then deploy the application */
  e.load_platform(argv[1]);
  e.load_deployment(argv[2]);

  /* Run the simulation */
  // Functions automatically end after completing tasks
  e.run();

  XBT_INFO("Simulation is over");

  return 0;
}