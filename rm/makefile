include ../makefile.inc

all: librm.a

# lib file dependencies
librm.a: librm.a(rm.o)  # and possibly other .o files

# c file dependencies
rm.o: rm.h

# dependencies to compile used libraries
.PHONY: ../rbf/librbf.a
../rbf/librbf.a:
	$(MAKE) -C ../rbf librbf.a


.PHONY: clean
clean:
	-rm *.a *.o
	-$(MAKE) -C ../rbf clean
