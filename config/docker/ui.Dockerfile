# syntax=docker/dockerfile:1.5
ARG NODE_VERSION=20-alpine

# Base Stage
FROM node:${NODE_VERSION} AS base
WORKDIR /app
# Note: we assume the build context is the root of the monorepo
COPY ui/package.json ui/package-lock.json ./
RUN npm ci

# Development Stage
FROM base AS development
COPY ui/ ./
EXPOSE 3333
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0"]

# Build Stage
FROM base AS build
COPY ui/ ./
RUN npm run build

# Production Stage
FROM nginx:stable-alpine AS production
COPY --from=build /app/dist /usr/share/nginx/html
# Add basic Nginx config for SPA routing
RUN printf "server {\n  listen 80;\n  location / {\n    root /usr/share/nginx/html;\n    index index.html;\n    try_files \$uri \$uri/ /index.html;\n  }\n}\n" > /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
