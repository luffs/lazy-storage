// frames.js - A raw WebSocket client for the adapter tests: says hello with
// permessage-deflate offered, sends the given messages, and reports each
// server frame's RSV1 bit (compressed or not) and payload size. What a
// runtime's own WebSocket hides.
import net from 'node:net';
import { randomBytes } from 'node:crypto';

const OPCODES = { 1: 'text', 2: 'binary', 8: 'close', 9: 'ping', 10: 'pong' };

/** One masked text frame, as a client must send them (up to 64 KB) */
function frame(text) {
  const payload = Buffer.from(text);
  if (payload.length > 0xffff) throw new Error('probe messages must stay under 64 KB');
  const extended = payload.length >= 126;
  const mask = randomBytes(4);
  const head = extended ? 4 : 2;
  const out = Buffer.alloc(head + 4 + payload.length);
  out[0] = 0x81;
  out[1] = 0x80 | (extended ? 126 : payload.length);
  if (extended) out.writeUInt16BE(payload.length, 2);
  mask.copy(out, head);
  for (let i = 0; i < payload.length; i++) out[head + 4 + i] = payload[i] ^ mask[i % 4];
  return out;
}

/**
 * @param {() => void} [options.after] - runs once the messages are in and
 *   handled, to cause server-side frames; those arrive as `caused`
 * @returns {Promise<{ extensions: string, frames: Frame[], caused: Frame[] }>}
 *   `extensions` is the server's Sec-WebSocket-Extensions header (empty when
 *   it accepted none); a frame's `text` is the payload of a plain text frame
 * @typedef {{ opcode: string, compressed: boolean, bytes: number, text: string }} Frame
 */
export function probeFrames(port, path, messages, { gap = 100, settle = 300, after } = {}) {
  return new Promise((resolve, reject) => {
    const frames = [];
    let mark = 0;   // how many frames had arrived when `after` ran
    let buffer = Buffer.alloc(0);
    let handshake = null;
    const socket = net.connect(port, 'localhost');
    socket.on('error', reject);
    const parse = () => {
      for (;;) {
        if (buffer.length < 2) return;
        let bytes = buffer[1] & 0x7f;
        let offset = 2;
        if (bytes === 126) {
          if (buffer.length < 4) return;
          bytes = buffer.readUInt16BE(2);
          offset = 4;
        } else if (bytes === 127) {
          if (buffer.length < 10) return;
          bytes = Number(buffer.readBigUInt64BE(2));
          offset = 10;
        }
        if (buffer.length < offset + bytes) return;
        const compressed = Boolean(buffer[0] & 0x40);
        const payload = buffer.subarray(offset, offset + bytes);
        frames.push({ opcode: OPCODES[buffer[0] & 0x0f] ?? String(buffer[0] & 0x0f), compressed, bytes, text: compressed ? '' : payload.toString('utf8') });
        buffer = buffer.subarray(offset + bytes);
      }
    };
    socket.on('data', chunk => {
      buffer = Buffer.concat([buffer, chunk]);
      if (handshake === null) {
        const end = buffer.indexOf('\r\n\r\n');
        if (end < 0) return;
        handshake = buffer.subarray(0, end).toString();
        buffer = buffer.subarray(end + 4);
        messages.forEach((m, i) => setTimeout(() => socket.write(frame(m)), i * gap));
        // Once the messages are in and handled (subscribed), let the caller cause server-side frames
        if (after) {
          setTimeout(() => {
            mark = frames.length;
            try { after(); } catch (err) { reject(err); }
          }, messages.length * gap + 50);
        }
        setTimeout(() => {
          const header = handshake.split('\r\n').find(h => /^sec-websocket-extensions:/i.test(h));
          socket.destroy();
          resolve({ extensions: header ? header.split(':').slice(1).join(':').trim() : '', frames, caused: frames.slice(mark) });
        }, messages.length * gap + settle);
      }
      parse();
    });
    socket.on('connect', () => {
      socket.write(
        `GET ${path} HTTP/1.1\r\nHost: localhost:${port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n` +
        `Sec-WebSocket-Key: ${randomBytes(16).toString('base64')}\r\nSec-WebSocket-Version: 13\r\n` +
        `Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\n\r\n`
      );
    });
  });
}
