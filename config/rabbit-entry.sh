#!/bin/sh
# All nodes of a RabbitMQ cluster must share the same Erlang cookie.
set -e
printf '%s' "$LAB_ERLANG_COOKIE" > /var/lib/rabbitmq/.erlang.cookie
chown rabbitmq:rabbitmq /var/lib/rabbitmq/.erlang.cookie
chmod 400 /var/lib/rabbitmq/.erlang.cookie
exec docker-entrypoint.sh rabbitmq-server
