<template>
  <form @submit.prevent="add">
    <input type="text" v-model="title" placeholder="Add a task" autocomplete="off" autofocus>
    <button>Add</button>
  </form>
  <ul>
    <li v-for="task in inOrder(state.tasks)" :key="task.id" :class="{ done: task.done }">
      <input type="checkbox" :checked="task.done" @change="toggle(task.id, $event.target.checked)">
      <span>{{ task.title }}</span>
      <button @click="remove(task.id)">x</button>
    </li>
  </ul>
  <p><button :disabled="!canUndo" @click="undo">Undo</button> <button :disabled="!canRedo" @click="redo">Redo</button></p>
</template>

<script>
import { inOrder } from 'lazy-storage';
import { useClient } from 'lazy-storage/vue';

/**
 * The list itself. `state` is a reactive mirror of the client's state:
 * every batch the client sees, local or remote, is applied to it in
 * place, which Vue tracks, so a teammate's edit renders exactly like our
 * own. Methods write to the client, never to the mirror. The mirror stops
 * following when the component unmounts.
 */
export default {
  name: 'SharedList',
  props: {
    db: { type: Object, required: true }
  },
  data() {
    return { ...useClient(this.db), title: '' };
  },
  methods: {
    inOrder,
    add() {
      const text = this.title.trim();
      if (!text) return;
      this.db.list('tasks').add({ title: text, done: false });  // at the end; an id is minted
      this.title = '';
    },
    toggle(id, done) { this.db.state.tasks[id].done = done; },
    remove(id) { this.db.list('tasks').remove(id); },
    undo() { this.db.undo(); },
    redo() { this.db.redo(); }
  }
};
</script>

<style scoped>
form { display: flex; gap: 0.5rem; margin: 1rem 0; }
input[type=text] { flex: 1; font: inherit; padding: 0.4rem 0.6rem; }
button { font: inherit; padding: 0.4rem 0.8rem; }
ul { list-style: none; padding: 0; }
li { display: flex; align-items: center; gap: 0.6rem; padding: 0.35rem 0; border-bottom: 1px solid #eee; }
li.done span { text-decoration: line-through; color: #999; }
li span { flex: 1; }
li button { padding: 0.1rem 0.5rem; }
</style>
