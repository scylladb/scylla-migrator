insist () {
  command=$1
  attempts=0
  max_attempts=240
  while ! eval "$command" ; do
      [[ $attempts -ge $max_attempts ]] && echo "Failed!" && exit 1
      attempts=$((attempts+1))
      sleep 1;
      echo "waiting... (${attempts}/${max_attempts})"
  done
}
