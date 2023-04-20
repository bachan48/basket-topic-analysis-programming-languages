#!/usr/bin/env bash
sbt --error 'set showSuccess := false' 'runMain stackoverflow.Topics data/documents.txt'

