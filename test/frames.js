// frames.js - A raw WebSocket client for the adapter tests: says hello with
// permessage-deflate offered, sends the given messages, and reports each
// server frame's RSV1 bit (compressed or not) and payload size. What a
// runtime's own WebSocket hides.
import net from 'node:net';
import { randomBytes } from 'node:crypto';

const OPCODES = { 1: 'text', 2: 'binary', 8: 'close', 9: 'ping', 10: 'pong' };

/** One masked text frame, as a client must send them */
function frame(text) {
  const payload = Buffer.from(text);
  if (payload.length >= 126) throw new Error('probe messages must stay under 126 bytes');
  const mask = randomBytes(4);
  const out = Buffer.alloc(6 + payload.length);
  out[0] = 0x81;
  out[1] = 0x80 | payload.length;
  mask.copy(out, 2);
  for (let i = 0; i < payload.length; i++) out[6 + i] = payload[i] ^ mask[i % 4];
  return out;
}

/**
 * @returns {Promise<{ extensions: string, frames: { opcode: string, compressed: boolean, bytes: number, text: string }[] }>}
 *   `extensions` is the server's Sec-WebSocket-Extensions header (empty when
 *   it accepted none); `text` is the payload of a plain text frame
 */
export function probeFrames(port, path, messages, { gap = 100, settle = 300 } = {}) {
  return new Promise((resolve, reject) => {
    const frames = [];
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
        setTimeout(() => {
          const header = handshake.split('\r\n').find(h => /^sec-websocket-extensions:/i.test(h));
          socket.destroy();
          resolve({ extensions: header ? header.split(':').slice(1).join(':').trim() : '', frames });
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
