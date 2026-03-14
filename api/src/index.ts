import express from 'express'
import { Pool } from 'pg'
import cors from 'cors'

const app = express()
const pool = new Pool({ connectionString: process.env.DATABASE_URL })

app.use(cors())

app.get('/articles', async (_req, res) => {
  const { rows } = await pool.query(`
    SELECT id, title, url, source, published_at
    FROM articles
    ORDER BY published_at DESC
    LIMIT 30
  `)
  res.json(rows)
})

app.get('/trends', async (_req, res) => {
  const { rows } = await pool.query(`
    SELECT DISTINCT ON (keyword) keyword, value, fetched_at
    FROM trends
    ORDER BY keyword, fetched_at DESC
  `)
  res.json(rows)
})

app.get('/digest', async (_req, res) => {
  const { rows } = await pool.query(`
    SELECT date, summary
    FROM digests
    WHERE date = CURRENT_DATE
  `)
  res.json(rows[0] ?? null)
})

app.get('/health', (_req, res) => {
  res.json({ ok: true })
})

const PORT = process.env.PORT ?? 3000
app.listen(PORT, () => console.log(`API running on ${PORT}`))
