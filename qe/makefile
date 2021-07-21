
include ../makefile.inc

all: libqe.a

# lib file dependencies
libqe.a: libqe.a(qe.o)  # and possibly other .o files

# c file dependencies
qe.o: qe.h

# dependencies to compile used libraries
.PHONY: ../qe/libqe.a
../qe/libqe.a:
	$(MAKE) -C ../qe libqe.a

.PHONY: clean
clean:
	-rm *.a *.o *~
	-$(MAKE) -C ../rm clean
	-$(MAKE) -C ../ix clean 
