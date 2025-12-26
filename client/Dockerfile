FROM node:20-alpine

WORKDIR /client

# Install dependencies first (cached layer)
COPY package.json package-lock.json ./
RUN npm install

# Copy the rest of the app
COPY . .

# Vite dev server
EXPOSE 5173
CMD ["npm", "run", "dev", "--", "--host"]
