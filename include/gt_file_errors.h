#pragma once

#include <exception>
#include <string>
#include <cstring>

class GTFileOpenError : public std::exception {
private:
  std::string errstr;
public:
  GTFileOpenError(std::string errstr) : errstr(errstr) {};
  virtual const char* what() const throw() {
    std::string temp = "Failed to open backing storage file! error=" + errstr + "\n";
    return temp.c_str();
  }
};

class GTFileReadError : public std::exception {
private:
  std::string errstr;
  int id;
public:
  GTFileReadError(std::string errstr, int id) : errstr(errstr), id(id) {};
  virtual const char* what() const throw() {
    std::string temp = "ERROR: failed to read from buffer " + std::to_string(id) + ". \"" + errstr + "\"\n";
    return temp.c_str();
  }
};

class GTFileWriteError : public std::exception {
private:
  std::string errstr;
  int id;
public:
  GTFileWriteError(std::string errstr, int id) : errstr(errstr), id(id) {};
  virtual const char* what() const throw() {
    std::string temp = "ERROR: flush failed to write to buffer " + std::to_string(id) + ". \"" + errstr + "\"\n";
    return temp.c_str();
  }
};
