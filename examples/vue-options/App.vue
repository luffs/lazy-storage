<template>
  <h1>Shared list (Vue SFC, Options API)</h1>
  <p class="status">{{ statusText }}</p>
  <SharedList :db="db" />
  <p class="presence">{{ presence.length ? 'Here now: ' + presence.join(', ') : '' }}</p>
</template>

<script>
import { markRaw } from 'vue';
import { createClient, sharedConnection, webSocketTransport, localStorageOutbox } from 'lazy-storage';
import { useClient } from 'lazy-storage/vue';
import SharedList from './SharedList.vue';

// Who we are: `?name=` in the URL, else what this browser remembers, else ask
const name = new URLSearchParams(location.search).get('name') || localStorage.getItem('example:name') || prompt('Your name?') || 'anonymous';
localStorage.setItem('example:name', name);

/**
 * The app owns the connection and the client, and hands the client to the
 * components that use it. `markRaw` keeps Vue from wrapping the client
 * (and lazy-storage's proxies inside it) in its own reactivity; what a
 * component shows of it comes from useClient, see SharedList.vue.
 */
export default {
  name: 'App',
  components: { SharedList },
  data() {
    // One socket per browser, shared with the other example pages' tabs (see basic/index.html)
    const connection = sharedConnection({
      name: 'example',
      transport: webSocketTransport(() => `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws?name=${encodeURIComponent(name)}`),
      storage: store => localStorageOutbox(`example:${store}`)
    });
    const db = createClient({ connection, store: 'shared', initial: { tasks: [] }, lists: ['tasks'] });
    const { status, presence } = useClient(db);   // refs, unwrapped as data; they stop with the component
    return { db: markRaw(db), status, presence };
  },
  computed: {
    statusText() {
      return this.status === 'online' ? `Online as ${name}` : this.status === 'connecting' ? 'Connecting…' : `Offline as ${name}`;
    }
  },
  created() {
    this.db.connect();
  },
  unmounted() {
    this.db.dispose();
  }
};
</script>

<style scoped>
h1 { font-size: 1.4rem; }
.status, .presence { color: #666; font-size: 0.9rem; min-height: 1.4em; }
</style>
