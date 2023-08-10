# Rengo
Rengo is a powerful and efficient MongoDB wire protocol implementation that seamlessly integrates MongoDB and Redis to enhance the performance of your applications. With Rengo, you can leverage the capabilities of both MongoDB and Redis caching without the need to modify or refactor your existing codebase.

## Prerequisites
- MongoDB 3.6 or higher
- Redis Stack
- Env variables for MongoDB(MONGO_URI) and Redis(REDIS_URI)


## Getting Started
- The binary file is available for linux x86_64 architecture. You can download the binary file from releases section.
- As mentioned in the prerequisites, you need to set the environment variables for MongoDB and Redis.
- Run the binary file and currently it will start listening on port 27017.
- Connect your application to Rengo and you are good to go.
- As this is an initial release, we are not supporting all the MongoDB commands. We will be adding more commands in the upcoming releases.
- Support of the open source community is highly appreciated. Please feel free to raise issues and contribute to the project.