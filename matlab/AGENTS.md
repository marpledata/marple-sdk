# MATLAB SDK Guide

This directory contains a small MATLAB client for Marple DB.

## Structure

- `DB.m`: MATLAB DB client implementation.
- `example.m`: example usage script.
- `config.json`: local configuration read by `DB.from_config()`.
- `README.md`: setup, quickstart, cache, and compatibility notes.

## Usage

- Add this directory to the MATLAB path before using the client:
  `addpath(genpath(fullfile(pwd, 'matlab')))`
- Create a client with `DB.from_config()` when using `config.json`.
- Run the example from the repo root with `run(fullfile('matlab', 'example.m'))`.
