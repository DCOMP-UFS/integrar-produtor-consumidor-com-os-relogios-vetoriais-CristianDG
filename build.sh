#! /usr/bin/env sh


out="./bin/out"

compile () {
  mpicc -o $out main.c -lpthread
}

run () {
  mpiexec -n 3 $out
}

case "$1" in
  "compile") compile ;;
  "run") compile && run ;;
  *) compile ;;
esac
