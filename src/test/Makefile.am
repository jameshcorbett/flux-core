AM_CFLAGS = \
	$(WARNING_CFLAGS) \
	$(CODE_COVERAGE_CFLAGS)

AM_LDFLAGS = \
	$(CODE_COVERAGE_LIBS)

AM_CPPFLAGS = \
	-I$(top_srcdir) \
	-I$(top_srcdir)/src/include \
	-I$(top_builddir)/src/common/libflux \
	$(ZMQ_CFLAGS)

noinst_SCRIPTS = \
	relnotes.sh \
	backtrace-all.sh \
	checks-annotate.sh \
	checks-lib.sh \
	checks_run.sh \
	cppcheck.sh \
	docker-deploy.sh \
	generate-matrix.py

LDADD = $(top_builddir)/src/common/libflux-internal.la \
	$(top_builddir)/src/common/libflux-core.la \
	$(ZMQ_LIBS) $(LIBPTHREAD) $(LIBDL)

EXTRA_DIST = $(noinst_SCRIPTS)
