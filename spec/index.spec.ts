
import { expect, test } from "bun:test";
import Fastify from 'fastify';
import { Pool } from 'pg';
import { randomUUID, createHash } from 'crypto';

const fastify = Fastify();
const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

type Output = {
  address: string;
  value: number;
};

type Input = {
  txId: string;
  index: number;
};

type Transaction = {
  id: string;
  inputs: Array<Input>;
  outputs: Array<Output>;
};

type Block = {
  id: string;
  height: number;
  transactions: Array<Transaction>;
};

fastify.post('/blocks', async (request, reply) => {
  const block: Block = request.body;

  // Validate block height
  const { rows: currentHeightRows } = await pool.query('SELECT MAX(height) as height FROM blocks');
  const currentHeight = currentHeightRows[0].height || 0;
  if (block.height !== currentHeight + 1) {
    return reply.status(400).send({ error: 'Invalid block height' });
  }

  for (const tx of block.transactions) {
    const inputSum = tx.inputs.reduce((sum, input) => sum + input.value, 0);
    const outputSum = tx.outputs.reduce((sum, output) => sum + output.value, 0);
    if (inputSum !== outputSum) {
      return reply.status(400).send({ error: 'Input and output sums do not match' });
    }
  }

  const hash = createHash('sha256');
  hash.update(block.height.toString());
  block.transactions.forEach(tx => hash.update(tx.id));
  const calculatedId = hash.digest('hex');
  if (block.id !== calculatedId) {
    return reply.status(400).send({ error: 'Invalid block ID' });
  }

  await pool.query('BEGIN');
  try {
    for (const tx of block.transactions) {
      for (const input of tx.inputs) {
        await pool.query('UPDATE balances SET balance = balance - $1 WHERE address = $2', [input.value, input.address]);
      }
      for (const output of tx.outputs) {
        await pool.query('UPDATE balances SET balance = balance + $1 WHERE address = $2', [output.value, output.address]);
      }
    }
    await pool.query('INSERT INTO blocks (id, height, data) VALUES ($1, $2, $3)', [block.id, block.height, JSON.stringify(block)]);
    await pool.query('COMMIT');
  } catch (err) {
    await pool.query('ROLLBACK');
    throw err;
  }

  return reply.send({ status: 'Block added' });
});

fastify.get('/balance/:address', async (request, reply) => {
  const { address } = request.params;
  const { rows } = await pool.query('SELECT balance FROM balances WHERE address = $1', [address]);
  if (rows.length === 0) {
    return reply.status(404).send({ error: 'Address not found' });
  }
  return reply.send({ balance: rows[0].balance });
});

fastify.post('/rollback', async (request, reply) => {
  const { height } = request.query;
  await pool.query('BEGIN');
  try {
    await pool.query('DELETE FROM blocks WHERE height > $1', [height]);
    // Recalculate balances
    await pool.query('TRUNCATE balances');
    const { rows: blocks } = await pool.query('SELECT data FROM blocks ORDER BY height');
    for (const row of blocks) {
      const block: Block = JSON.parse(row.data);
      for (const tx of block.transactions) {
        for (const input of tx.inputs) {
          await pool.query('UPDATE balances SET balance = balance - $1 WHERE address = $2', [input.value, input.address]);
        }
        for (const output of tx.outputs) {
          await pool.query('UPDATE balances SET balance = balance + $1 WHERE address = $2', [output.value, output.address]);
        }
      }
    }
    await pool.query('COMMIT');
  } catch (err) {
    await pool.query('ROLLBACK');
    throw err;
  }
  return reply.send({ status: 'Rollback completed' });
});

test('POST /blocks', async () => {
  const response = await fastify.inject({
    method: 'POST',
    url: '/blocks',
    payload: {
      id: 'block1',
      height: 1,
      transactions: [{
        id: 'tx1',
        inputs: [],
        outputs: [{ address: 'addr1', value: 10 }]
      }]
    }
  });
  expect(response.statusCode).toBe(200);
  expect(response.json()).toEqual({ status: 'Block added' });
});

test('GET /balance/:address', async () => {
  const response = await fastify.inject({
    method: 'GET',
    url: '/balance/addr1'
  });
  expect(response.statusCode).toBe(200);
  expect(response.json()).toEqual({ balance: 10 });
});

test('POST /rollback', async () => {
  const response = await fastify.inject({
    method: 'POST',
    url: '/rollback?height=1'
  });
  expect(response.statusCode).toBe(200);
  expect(response.json()).toEqual({ status: 'Rollback completed' });
});