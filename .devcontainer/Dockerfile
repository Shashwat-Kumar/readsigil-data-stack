FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm
# The base image ships a Yarn apt source whose signing key has rotated; apt-get update
# fails on it, which breaks the Docker-in-Docker feature. Harbour does not need Yarn.
RUN rm -f /etc/apt/sources.list.d/yarn.list
