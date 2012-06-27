require 'socket'
module IProto
  # TODO: timeouts
  class TCPSocket < ::TCPSocket
    include IProto::ConnectionAPI

    # begin ConnectionAPI
    def send_request(request_type, body)
      request_id = next_request_id
      r = send [request_type, body.bytesize, request_id].pack(PACK) + body, 0
      response_size = recv_header request_id
      recv_response response_size
    rescue Errno::EPIPE => e
      raise Disconnected, e
    end
    # end ConnectionAPI

    def recv_header(request_id)
      header = read(12)  or raise Disconnected, 'disconnected while read'
      type, response_size, recv_request_id = header.unpack(PACK)
      raise UnexpectedResponse.new("Waiting response for request_id #{request_id}, but received for #{recv_request_id}") unless request_id == recv_request_id
      response_size
    end

    def recv_response(response_size)
      read(response_size)  or raise Disconnected, 'disconnected while read'
    end
  end
end
