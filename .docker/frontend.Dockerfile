FROM node:22-bookworm-slim

WORKDIR /app
ENV NEXT_TELEMETRY_DISABLED=1
COPY examples/frontend-next/package.json ./
COPY examples/frontend-next/package-lock.json ./
RUN npm ci
COPY examples/frontend-next .
RUN npm run build

USER node

CMD ["npm", "run", "start", "--", "--hostname", "0.0.0.0", "--port", "3000"]
