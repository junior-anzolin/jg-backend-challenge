FROM oven/bun:1-alpine AS base
WORKDIR /usr/src/app

# Install all dependencies for development and building.
FROM base AS install
WORKDIR /temp/dev
COPY package.json bun.lock* ./
RUN bun install --frozen-lockfile

# Install only runtime dependencies for production.
FROM base AS install-production
WORKDIR /temp/prod
COPY package.json bun.lock* ./
RUN bun install --production --frozen-lockfile

# Development image: source code and NestJS watch mode.
FROM base AS development
COPY --from=install /temp/dev/node_modules ./node_modules
COPY . .
ENV NODE_ENV=development
EXPOSE 3000
CMD ["bun", "run", "start:dev"]

# Build the application once for the production image.
FROM base AS build
COPY --from=install /temp/dev/node_modules ./node_modules
COPY . .
ENV NODE_ENV=production
RUN bun run build

# Production image: compiled application and runtime dependencies.
FROM base AS release
ENV NODE_ENV=production
COPY --from=install-production /temp/prod/node_modules ./node_modules
COPY --from=build /usr/src/app/dist ./dist
COPY package.json ./package.json
USER bun
EXPOSE 3000
CMD ["bun", "run", "dist/main.js"]