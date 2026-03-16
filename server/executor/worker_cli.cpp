
#include <iostream>
#include <unistd.h>

int main(int argc, char ** argv)
{
  std::cout << "Worker process started" << std::endl;
  sleep(5);
  std::cout << "Wordker process ended" << std::endl;
  return 0;
}
