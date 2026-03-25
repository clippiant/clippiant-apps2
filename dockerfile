FROM node:20-bullseye

RUN apt-get update && apt-get install -y ffmpeg fonts-dejavu-core && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json package-lock.json* ./
FROM node:20-bullseye

RUN apt-get update && apt-get install -y ffmpeg fonts-dejavu-core && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json package-lock.json* ./
RUN echo "=== PACKAGE.JSON USED IN BUILD ===" && cat package.json
RUN npm install
RUN echo "=== INSTALLED PACKAGES CHECK ===" && npm ls @runwayml/sdk @fal-ai/client || true

COPY . .

ENV NODE_ENV=production

CMD ["npm", "start"]
