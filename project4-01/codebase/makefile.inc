## For students: change this path to the root of your code
CODEROOT = ..

#LDLIBS = -lreadline

#CC = gcc
CC = g++
CXX = $(CC)


#CPPFLAGS = -Wall -I$(CODEROOT) -g -std=c++11  # with debugging info and the C++11 feature
CPPFLAGS = -Wall -I$(CODEROOT) -g -std=c++0x  # with debugging info and the C++11 feature
