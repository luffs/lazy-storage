<template>
  <h1>Shared list (Vue SFC, Options API)</h1>
  <p class="status">{{ statusText }}</p>
  <SharedList :db="db" />
  <p class="presence">{{ presence.length ? 'Here now: ' + presence.join(', ') : '' }}</p>
</template>

<script>
import { markRaw } from 'vue';
import { createClient, createConnection, webSocketTransport, localStorageOutbox } from 'lazy-storage';
import SharedList from './SharedList.vue';

// Who we are: `?name=` in the URL, else what this browser remembers, else ask
const name = new URLSearchParams(location.search).get('name') || localStorage.getItem('example:name') || prompt('Your name?') || 'anonymous';
localStorage.setItem('example:name', name);

/**
 * The app owns the connection and the client, and hands the client to the
 * components that use it. `markRaw` keeps Vue from wrapping the client
 * (and lazy-storage's proxies inside it) in its own reactivity; components
 * mirror what they need from it, see SharedList.vue.
 */
export default {
  name: 'App',
  components: { SharedList },
  data() {
    const connection = createConnection({
      transport: webSocketTransport(() => `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws?name=${encodeURIComponent(name)}`)
    });
    const db = createClient({ connection, store: 'shared', initial: { tasks: [] }, lists: ['tasks'], storage: localStorageOutbox('example:vue-sfc') });
    return { db: markRaw(db), status: db.status, presence: [] };
  },
  computed: {
    statusText() {
      return this.status === 'online' ? `Online as ${name}` : this.status === 'connecting' ? 'Connecting…' : `Offline as ${name}`;
    }
  },
  created() {
    this.stop = [
      this.db.on('status', status => { this.status = status; }),
      this.db.on('presence', users => { this.presence = users; })
    ];
    this.db.connect();
  },
  unmounted() {
    for (const stop of this.stop) stop();
    this.db.dispose();
  }
};
</script>

<style scoped>
h1 { font-size: 1.4rem; }
.status, .presence { color: #666; font-size: 0.9rem; min-height: 1.4em; }
</style>
