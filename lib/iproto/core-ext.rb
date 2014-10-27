require 'fiber'
class Fiber
  alias call resume
end

class String
  unless method_defined?(:b)
    def b
      force_encoding(::Encoding::BINARY)
    end
  end
end
