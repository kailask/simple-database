
include ../makefile.inc

all: libix.a

# lib file dependencies
libix.a: libix.a(ix.o)  # and possibly other .o files

# c file dependencies
ix.o: ix.h

# dependencies to compile used libraries
.PHONY: ../rbf/librbf.a
../rbf/librbf.a:
	$(MAKE) -C ../rbf librbf.a

.PHONY: clean
clean:
	-rm *.o *.a