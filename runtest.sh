#!/bin/bash

while (true); do
  mvn exec:java@leak
  echo "will run test again in 5s..."
  sleep 5s
done