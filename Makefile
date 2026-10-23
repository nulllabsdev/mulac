.PHONY: all fmt check test test-libs test-apps

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

check:
	$(call run-in-dirs,$(CARGO_DIRS),check)

# Run every crate, including the DB-backed test apps (needs Postgres).
test:
	$(call run-in-dirs,$(CARGO_DIRS),test --all-features)

# Library crates only (kernel + libs); no database required.
test-libs:
	$(call run-in-dirs,$(LIB_DIRS),test --all-features)

# Test apps only (todo, twitter); needs Postgres running.
test-apps:
	$(call run-in-dirs,$(APP_DIRS),test --all-features)

CI_DIRS := libs kernel
NEW_LIBS := inbox_api commanding_api eventing_api outbox_api mulac_poem mulac_axum
LIBS_MANIFEST := --manifest-path libs/Cargo.toml
# Opt-in adapter features; feature `<name>` enables the optional `<name>_api` crate.
ADAPTER_FEATURES := commanding eventing outbox
ADAPTERS := mulac_poem mulac_axum

# Format, lint and test libs and kernel; test_apps need running infrastructure.
# Existing crates still have clippy warnings, so only new crates deny them.
ci:
	@set -e; \
	for dir in $(CI_DIRS); do \
		echo "== $$dir =="; \
		cargo fmt --all --check --manifest-path "$$dir/Cargo.toml"; \
		cargo clippy --workspace --all-targets --all-features --manifest-path "$$dir/Cargo.toml"; \
		cargo test --workspace --all-features --manifest-path "$$dir/Cargo.toml"; \
	done; \
	for crate in $(NEW_LIBS); do \
		echo "== $$crate =="; \
		cargo clippy --no-deps -p $$crate --all-targets --all-features $(LIBS_MANIFEST) -- -D warnings; \
		cargo clippy --no-deps -p $$crate --all-targets --no-default-features $(LIBS_MANIFEST) -- -D warnings; \
	done; \
	echo "== dependency boundaries =="; \
	if cargo tree -p inbox_api --no-default-features --features tokio -e normal $(LIBS_MANIFEST) | grep -q diesel; then \
		echo "inbox_api without diesel must not depend on Diesel"; exit 1; fi; \
	if cargo tree -p mulac_poem --all-features -e normal $(LIBS_MANIFEST) | grep -qE ' (axum|utoipa) '; then \
		echo "mulac_poem must not depend on Axum"; exit 1; fi; \
	if cargo tree -p mulac_axum --all-features -e normal $(LIBS_MANIFEST) | grep -qE ' poem'; then \
		echo "mulac_axum must not depend on Poem"; exit 1; fi; \
	if cargo tree -p mulac_axum -e normal $(LIBS_MANIFEST) | grep -q ' utoipa '; then \
		echo "mulac_axum must not require utoipa"; exit 1; fi; \
	if cargo tree -p outbox_api --all-features -e normal $(LIBS_MANIFEST) | grep -q ' lapin '; then \
		echo "outbox_api must not depend on lapin"; exit 1; fi; \
	for adapter in $(ADAPTERS); do \
		if cargo tree -p $$adapter --all-features -e normal $(LIBS_MANIFEST) | grep -q ' lapin '; then \
			echo "$$adapter must not depend on lapin"; exit 1; fi; \
	done; \
	for feature in $(ADAPTER_FEATURES); do \
		api=$${feature}_api; \
		echo "== adapter feature $$feature =="; \
		if cargo tree -p $$api --no-default-features --features tokio -e normal $(LIBS_MANIFEST) | grep -q diesel; then \
			echo "$$api must not depend on Diesel"; exit 1; fi; \
		for adapter in $(ADAPTERS); do \
			cargo clippy --no-deps -p $$adapter --all-targets --no-default-features --features $$feature $(LIBS_MANIFEST) -- -D warnings; \
			if cargo tree -p $$adapter -e normal $(LIBS_MANIFEST) | grep -q " $$api v"; then \
				echo "$$adapter must not include $$api by default"; exit 1; fi; \
			if cargo tree -p $$adapter --no-default-features --features $$feature -e normal $(LIBS_MANIFEST) | grep -q diesel; then \
				echo "$$adapter with only $$feature must not depend on Diesel"; exit 1; fi; \
		done; \
		cargo clippy --no-deps -p mulac_axum --all-targets --no-default-features --features $$feature,utoipa $(LIBS_MANIFEST) -- -D warnings; \
	done

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
	$(call publish-lib,mulac_diesel,mulac_diesel)
	$(call publish-lib,eventing,mulac-eventing)
	$(call publish-lib,commanding,mulac-commanding)
	$(call publish-lib,outbox,mulac-outbox)
	$(call publish-lib,inbox,mulac-inbox)
	$(call publish-lib,inbox_api,inbox_api)
	$(call publish-lib,commanding_api,commanding_api)
	$(call publish-lib,eventing_api,eventing_api)
	$(call publish-lib,outbox_api,outbox_api)
	$(call publish-lib,mulac_poem,mulac_poem)
	$(call publish-lib,mulac_axum,mulac_axum)
pack_and_publish:
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
	cargo package --no-verify --manifest-path libs/inbox_api/Cargo.toml
	$(MAKE) -C $(MARGO_PROJECT) add CRATE=$(CURDIR)/libs/target/package/inbox_api-$(LIBS_VERSION).crate
	cargo package --no-verify --manifest-path libs/commanding_api/Cargo.toml
	$(MAKE) -C $(MARGO_PROJECT) add CRATE=$(CURDIR)/libs/target/package/commanding_api-$(LIBS_VERSION).crate
	cargo package --no-verify --manifest-path libs/eventing_api/Cargo.toml
	$(MAKE) -C $(MARGO_PROJECT) add CRATE=$(CURDIR)/libs/target/package/eventing_api-$(LIBS_VERSION).crate
	cargo package --no-verify --manifest-path libs/outbox_api/Cargo.toml
	$(MAKE) -C $(MARGO_PROJECT) add CRATE=$(CURDIR)/libs/target/package/outbox_api-$(LIBS_VERSION).crate
	cargo package --no-verify --manifest-path libs/mulac_poem/Cargo.toml
	$(MAKE) -C $(MARGO_PROJECT) add CRATE=$(CURDIR)/libs/target/package/mulac_poem-$(LIBS_VERSION).crate
	cargo package --no-verify --manifest-path libs/mulac_axum/Cargo.toml
	$(MAKE) -C $(MARGO_PROJECT) add CRATE=$(CURDIR)/libs/target/package/mulac_axum-$(LIBS_VERSION).crate
	cargo package --no-verify --manifest-path kernel/Cargo.toml
	$(MAKE) -C $(MARGO_PROJECT) add CRATE=$(CURDIR)/kernel/target/package/mulac-kernel-$(KERNEL_VERSION).crate

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
