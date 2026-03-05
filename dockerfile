FROM node:20-bullseye

RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install
COPY . .

ENV NODE_ENV=production
CMD ["npm", "start"]