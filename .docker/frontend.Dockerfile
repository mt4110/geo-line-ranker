FROM node:22-bookworm-slim

WORKDIR /app
ARG NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:4000
ENV NEXT_TELEMETRY_DISABLED=1
COPY examples/frontend-next/package.json ./
COPY examples/frontend-next/package-lock.json ./
RUN npm ci
COPY examples/frontend-next .
RUN NEXT_PUBLIC_API_BASE_URL=${NEXT_PUBLIC_API_BASE_URL} npm run build

USER node

CMD ["./node_modules/.bin/next", "start", "--hostname", "0.0.0.0", "--port", "3000"]
