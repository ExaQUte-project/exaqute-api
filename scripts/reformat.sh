#!/bin/bash

cd `dirname $0`/..
isort --profile black exaqute tests
black exaqute tests
flake8 exaqute tests --ignore=E203,W503
