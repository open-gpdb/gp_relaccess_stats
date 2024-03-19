MODULE_big      = gp_relaccess_stats
OBJS            = ./src/gp_relaccess_stats.o
EXTENSION       = gp_relaccess_stats
EXTVERSION      = 1.0
DATA            = $(wildcard sql/*--*.sql)
#REGRESS         = gp_relaccess_stats
#REGRESS_OPTS    = --inputdir=test/
PGFILEDESC      = "gp_relaccess_stats - facility to track how and when tables, partitions or views were accessed"
PG_CXXFLAGS     += $(COMMON_CPP_FLAGS)
PG_CONFIG       = pg_config
PGXS            := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
