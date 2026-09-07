# Dockerfile
FROM node:22-alpine

# Create app directory
WORKDIR /usr/src/app

# Copy package.json first to leverage caching
COPY package.json ./
RUN npm install

# Copy app code
COPY src/ ./src/

# Expose port used by server (default 3000)
EXPOSE 3000

# Run the server
CMD ["node", "src/server.js"]
