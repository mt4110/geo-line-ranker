FROM node:22-bookworm

WORKDIR /app
COPY examples/frontend-next/package.json ./
COPY examples/frontend-next/package-lock.json ./
RUN npm ci
COPY examples/frontend-next .

CMD ["npm", "run", "dev", "--", "--hostname", "0.0.0.0", "--port", "3000"]
