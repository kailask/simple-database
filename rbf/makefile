include ../makefile.inc

all: librbf.a

# c file dependencies
pfm.o: pfm.h
rbfm.o: rbfm.h

# lib file dependencies
librbf.a: librbf.a(pfm.o)  # and possibly other .o files
librbf.a: librbf.a(rbfm.o)

# dependencies to compile used libraries
.PHONY: ../rbf/librbf.a
../rbf/librbf.a:
	$(MAKE) -C ../rbf librbf.a

.PHONY: clean
clean:
	-rm *.a *.o
