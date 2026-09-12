// register-delete.test.js - A key removed from inside a register's value is removed everywhere:
// a register travels as a whole value, and its whole value is what it becomes
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from '../src/server/index.js';
import { registerSet } from '../src/core/paths.js';
import { replacingRegisters } from '../src/core/model.js';
import { createNetwork } from './helpers.js';

const INITIAL = { profile: {}, tasks: {} };
const REGISTERS = ['profile'];

test('replacingRegisters turns a register\'s new value into a patch that removes what the value there lacks', () => {
  const regs = registerSet(['profile', 'tasks/*/meta']);
  const state = { profile: { name: 'Ann', nick: 'annie', prefs: { dark: true, wide: false } }, tasks: { t1: { title: 'x', meta: { a: 1 } } } };
  assert.deepEqual(replacingRegisters({ profile: { name: 'Ann', prefs: { dark: true } } }, regs, state),
    { profile: { nick: null, name: 'Ann', prefs: { wide: null, dark: true } } });
  assert.deepEqual(replacingRegisters({ profile: { name: 'Bee' }, tasks: { t1: { title: 'y', meta: {} } } }, regs, state),
    { profile: { nick: null, prefs: null, name: 'Bee' }, tasks: { t1: { title: 'y', meta: { a: null } } } });
  // Nothing there yet, a value that is no object, or a deletion: as they are
  assert.deepEqual(replacingRegisters({ tasks: { t2: { meta: { b: 2 } } } }, regs, state), { tasks: { t2: { meta: { b: 2 } } } });
  assert.deepEqual(replacingRegisters({ profile: null }, regs, state), { profile: null });
  assert.deepEqual(replacingRegisters({ profile: 'gone' }, regs, state), { profile: 'gone' });
});

test('a key deleted inside a register goes from the server and the other clients, as the whole value it is', async () => {
  const store = createStore({ initial: INITIAL, registers: REGISTERS });
  const net = createNetwork(store);
  const a = net.client({ replicaId: 'a', initial: INITIAL, registers: REGISTERS });
  const b = net.client({ replicaId: 'b', initial: INITIAL, registers: REGISTERS });
  await net.settle();
  a.state.profile = { name: 'Ann', nick: 'annie' };
  await net.settle();
  assert.deepEqual(b.state.profile, { name: 'Ann', nick: 'annie' });

  delete a.state.profile.nick;
  await net.settle();
  assert.deepEqual(store.snapshot().profile, { name: 'Ann' }, 'the server has the register as a whole');
  assert.deepEqual(b.state.profile, { name: 'Ann' }, 'and so does the other client');

  a.state.profile = { name: 'Ann', nick: 'annie' };
  await net.settle();
  a.state.profile = { name: 'Bee' };
  await net.settle();
  assert.deepEqual(store.snapshot().profile, { name: 'Bee' });
  assert.deepEqual(b.state.profile, { name: 'Bee' }, 'a value assigned whole replaces the old one');

  // The server's own write of a register replaces it too
  store.patch({ profile: { name: 'Cee', role: 'admin' } });
  await net.settle();
  assert.deepEqual(store.snapshot().profile, { name: 'Cee', role: 'admin' });
  assert.deepEqual(a.state.profile, { name: 'Cee', role: 'admin' });
  store.patch({ profile: { name: 'Cee' } });
  await net.settle();
  assert.deepEqual(store.snapshot().profile, { name: 'Cee' });
  assert.deepEqual(b.state.profile, { name: 'Cee' });
  a.dispose();
  b.dispose();
});
