FROM node:26-slim

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

COPY src ./src

RUN mkdir -p runs repros && chown -R node:node /app

ENV NODE_ENV=production
USER node
EXPOSE 8080
CMD ["node", "src/server.ts"]
