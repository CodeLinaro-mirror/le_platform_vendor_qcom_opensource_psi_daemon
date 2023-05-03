AM_CPPFLAGS := \
    -W \
    -Wall \
    -Wextra \
    -Wunused \
    -Werror
ACLOCAL_AMFLAGS = -I m4

#Build psi_daemon binary
bin_PROGRAMS := psi_daemon
psi_daemon_SOURCES := psi_daemon.cpp memory_plugin_interface.cpp
psi_daemon_CPPFLAGS := $(AM_CPPFLAGS) @LIBBASE_CFLAGS@ @LIBVMMEM_CFLAGS@
psi_daemon_LDFLAGS := -lbase -lpthread
psi_daemon_LDADD = @LIBVMMEM_LIBS@

pkgconfigdir := $(libdir)/pkgconfig
