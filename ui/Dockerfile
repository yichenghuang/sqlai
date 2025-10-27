# =============================
# 1. Install *all* dependencies
# =============================
FROM node:20-alpine AS builder
WORKDIR /app

# Copy only lockfiles – fastest cache hit
COPY package.json package-lock.json ./

# Reproducible install
RUN npm ci

COPY . .
# Build (Next.js reads from /src automatically if configured)
RUN npm run build   # generates .next/standalone

# =============================
# Standalone runtime (tiny)
# =============================
FROM node:20-alpine AS runner
WORKDIR /app

ENV NODE_ENV=production

# Copy the self-contained folder that Next.js creates
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public

EXPOSE 3000
CMD ["node", "server.js"]