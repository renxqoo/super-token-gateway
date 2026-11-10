FROM oven/bun:1.4.4@sha256:1d22548f177f4ff38e0c15baa3c2c46c6c2060ba2d1bc573bb4720b407548f1d AS build
WORKDIR /build
COPY package.json bun.lock bunfig.toml tsconfig.json ./
COPY apps ./apps
COPY packages ./packages
COPY scripts ./scripts
RUN bun install --frozen-lockfile && bun run build
FROM oven/bun:1.4.4@sha256:1d22548f177f4ff38e0c15baa3c2c46c6c2060ba2d1bc573bb4720b407548f1d
WORKDIR /app
COPY --from=build --chown=bun:bun /build/dist ./dist
USER bun
ENV NODE_ENV=production HOST=0.0.0.0 PORT=8088
EXPOSE 8088
STOPSIGNAL SIGTERM
CMD ["bun","dist/gateway/main.js"]
