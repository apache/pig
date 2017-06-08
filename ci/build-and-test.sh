#!/usr/bin/env bash
ant -Dhadoopversion=23 test-commit -Dtest.junit.output.format=xml
ant -Dhadoopversion=23 test-core -Dtest.junit.output.format=xml
