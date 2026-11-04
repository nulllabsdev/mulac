.PHONY: all fmt check test test-libs test-apps prune-target

CARGO_DIRS := $(shell find . -name Cargo.toml -not -path '*/target/*' -exec dirname {} \; | sort)
LIB_DIRS   := $(filter-out ./test_apps/%,$(CARGO_DIRS))
APP_DIRS   := $(filter     ./test_apps/%,$(CARGO_DIRS))

KERNEL_VERSION := $(shell cargo pkgid --manifest-path kernel/Cargo.toml | sed 's/.*[#@]//')
LIBS_VERSION   := $(shell awk -F'"' '/^version/{print $$2; exit}' libs/Cargo.toml)

define run-fmt-in-crates
	@set -e; \
	for dir in $(CARGO_DIRS); do \
		echo "== $$dir =="; \
		if grep -q '^\[workspace\]' "$$dir/Cargo.toml"; then \
			cargo fmt --all --manifest-path "$$dir/Cargo.toml"; \
		else \
			cargo fmt --manifest-path "$$dir/Cargo.toml"; \
		fi; \
	done
endef

# $(1) = space-separated crate dirs, $(2) = cargo subcommand (+ flags)
define run-in-dirs
	@set -e; \
	for dir in $(1); do \
		echo "== $$dir =="; \
		cargo $(2) --manifest-path "$$dir/Cargo.toml"; \
	done
endef

fmt:
	$(call run-fmt-in-crates)

.PHONY: check-prelude
check-prelude:
	python3 scripts/check_prelude.py

check: check-prelude
	$(call run-in-dirs,$(CARGO_DIRS),check)

# Run every crate, including the DB-backed test apps (needs Postgres).
test:
	$(call run-in-dirs,$(CARGO_DIRS),test --all-features)

# Library crates only (kernel + libs); no database required.
test-libs:
	cargo test --manifest-path test_apps/http_adapters/Cargo.toml
	$(call run-in-dirs,$(LIB_DIRS),test --all-features)

# Test apps only (todo, twitter); needs Postgres running.
test-apps:
	$(call run-in-dirs,$(APP_DIRS),test --all-features)

CI_DIRS := libs kernel test_apps/http_adapters
LIBS_MANIFEST := --manifest-path libs/Cargo.toml

# Test every published library and the database-free combined HTTP harness.
ci: check-prelude
	@set -e; \
	for dir in $(CI_DIRS); do \
		cargo fmt --all --check --manifest-path "$$dir/Cargo.toml"; \
		cargo clippy --workspace --all-targets --all-features --manifest-path "$$dir/Cargo.toml"; \
		cargo test --workspace --all-features --manifest-path "$$dir/Cargo.toml"; \
	done
	cargo clippy -p mulac_http --all-targets --all-features $(LIBS_MANIFEST) --no-deps -- -D warnings
	cargo clippy --manifest-path test_apps/http_adapters/Cargo.toml --all-targets --no-deps -- -D warnings
	python3 scripts/check_http_features.py

MARGO_PROJECT := /home/nulllabs/work/labs/margo

define publish-lib
	cargo package --no-verify --manifest-path libs/$(1)/Cargo.toml
	$(MAKE) -C $(MARGO_PROJECT) add CRATE=$(CURDIR)/libs/target/package/$(2)-$(LIBS_VERSION).crate
endef

package:
	@echo "== libs v$(LIBS_VERSION) =="
	cargo package --no-verify --manifest-path libs/Cargo.toml --workspace
	@echo "== kernel v$(KERNEL_VERSION) =="
	cargo package --no-verify --manifest-path kernel/Cargo.toml

publish:
	$(call publish-lib,mulac_http,mulac_http)
	$(call publish-lib,mulac_diesel,mulac_diesel)
	$(call publish-lib,eventing,mulac-eventing)
	$(call publish-lib,commanding,mulac-commanding)
	$(call publish-lib,outbox,mulac-outbox)
	$(call publish-lib,inbox,mulac-inbox)
pack_and_publish:
	cargo package --no-verify --manifest-path libs/mulac_http/Cargo.toml
	$(MAKE) -C $(MARGO_PROJECT) add CRATE=$(CURDIR)/libs/target/package/mulac_http-$(LIBS_VERSION).crate
	cargo package --no-verify --manifest-path libs/mulac_diesel/Cargo.toml
	$(MAKE) -C $(MARGO_PROJECT) add CRATE=$(CURDIR)/libs/target/package/mulac_diesel-$(LIBS_VERSION).crate
	cargo package --no-verify --manifest-path libs/eventing/Cargo.toml
	$(MAKE) -C $(MARGO_PROJECT) add CRATE=$(CURDIR)/libs/target/package/mulac-eventing-$(LIBS_VERSION).crate
	cargo package --no-verify --manifest-path libs/commanding/Cargo.toml
	$(MAKE) -C $(MARGO_PROJECT) add CRATE=$(CURDIR)/libs/target/package/mulac-commanding-$(LIBS_VERSION).crate
	cargo package --no-verify --manifest-path libs/outbox/Cargo.toml
	$(MAKE) -C $(MARGO_PROJECT) add CRATE=$(CURDIR)/libs/target/package/mulac-outbox-$(LIBS_VERSION).crate
	cargo package --no-verify --manifest-path libs/inbox/Cargo.toml
	$(MAKE) -C $(MARGO_PROJECT) add CRATE=$(CURDIR)/libs/target/package/mulac-inbox-$(LIBS_VERSION).crate
	cargo package --no-verify --manifest-path kernel/Cargo.toml
	$(MAKE) -C $(MARGO_PROJECT) add CRATE=$(CURDIR)/kernel/target/package/mulac-kernel-$(KERNEL_VERSION).crate

# Keep only the newest KEEP builds per package in every target/ dir.
# `make prune-target KEEP=3`, or `make prune-target DRY_RUN=1` to only list what would go.
KEEP ?= 2
prune-target:
	@find . -path ./.git -prune -o -type d -regex '.*/target/[^/]*/\(deps\|\.fingerprint\|build\|incremental\)' -prune -print | \
	while read -r d; do \
		find "$$d" -mindepth 1 -maxdepth 1 -printf '%T@\t%p\n' | sort -rn | \
		awk -F'\t' -v N=$(KEEP) '{p=$$2; f=p; sub(/.*\//,"",f); b=f; sub(/\..*/,"",b); h=b; sub(/.*-/,"",h); n=b; sub(/-[^-]*$$/,"",n); if (f ~ /\.(rlib|rmeta|so|a|dylib)$$/) sub(/^lib/,"",n); k=n SUBSEP h; if (!(k in s)) s[k]=++c[n]; if (s[k]>N) print p}'; \
	done | \
	if [ -n "$(DRY_RUN)" ]; then cat; else xargs -r -d '\n' rm -rf; fi

test-apps-up:
	@$(MAKE) -C test_apps up

test-apps-down:
	@$(MAKE) -C test_apps down

test-apps-reset:
	@$(MAKE) -C test_apps reset

test-apps-test:
	@$(MAKE) -C test_apps/todo test
	@$(MAKE) -C test_apps/twitter test

test-apps-serve:
	@$(MAKE) -C test_apps/todo serve
	@$(MAKE) -C test_apps/twitter serve
