FROM node:20-alpine

# Create app directory
WORKDIR /app

# Copy package.json and install dependencies first (better layer caching)
COPY package*.json ./
RUN npm install --only=production

# Copy source code
COPY . .

CMD ["npm", "start"]