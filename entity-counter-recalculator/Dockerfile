# Stage 1 - build function and dependencies
FROM python:3.11 AS build-image

RUN apt-get update

# Install Lambda Runtime Interface Client for Python
RUN pip install awslambdaric --target /home/app/

# Copy and install other requirements
COPY minimalRequirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -r minimalRequirements.txt --target /home/app/

# Create function directory
RUN mkdir -p /home/app/
# Copy handler function
COPY main.py /home/app/
COPY /app /home/app/app

# Stage 2 - final runtime image
# Grab a fresh copy of the Python image
FROM python:3.11
# Set working directory to function root directory
WORKDIR /home/app
# Copy in the built dependencies
COPY --from=build-image /home/app /home/app
# (Optional) Add Lambda Runtime Interface Emulator and use a script in the ENTRYPOINT for simpler local runs
ADD https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie /usr/bin/aws-lambda-rie
COPY entry.sh /
RUN chmod 755 /usr/bin/aws-lambda-rie /entry.sh
ENTRYPOINT [ "/entry.sh" ]
CMD [ "main.handler" ]
