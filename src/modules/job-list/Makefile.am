AM_CFLAGS = \
	$(WARNING_CFLAGS) \
	$(CODE_COVERAGE_CFLAGS)

AM_LDFLAGS = \
	$(CODE_COVERAGE_LIBS)

AM_CPPFLAGS = \
	-I$(top_srcdir) \
	-I$(top_srcdir)/src/include \
	-I$(top_builddir)/src/common/libflux \
	$(ZMQ_CFLAGS) $(FLUX_SECURITY_CFLAGS) $(YAMLCPP_CFLAGS)

fluxmod_LTLIBRARIES = job-list.la

job_list_la_SOURCES = \
	job-list.c \
	job-list.h \
	job_state.h \
	job_state.c \
	list.h \
	list.c \
	job_util.h \
	job_util.c \
	idsync.h \
	idsync.c \
	stats.h \
	stats.c

job_list_la_LDFLAGS = $(fluxmod_ldflags) -module
job_list_la_LIBADD = $(fluxmod_libadd) \
	$(top_builddir)/src/common/libjob/libjob.la \
	$(top_builddir)/src/common/libflux-internal.la \
	$(top_builddir)/src/common/libflux-core.la \
	$(top_builddir)/src/common/libflux-optparse.la \
	$(top_builddir)/src/common/librlist/librlist.la \
	$(ZMQ_LIBS) \
	$(HWLOC_LIBS)
