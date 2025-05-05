local BintUtils = { _version = "0.0.1" }
local bint = require('.bint')(256)

BintUtils.add = function(a, b)
  return tostring(bint(a) + bint(b))
end
BintUtils.subtract = function(a, b)
  return tostring(bint(a) - bint(b))
end
BintUtils.multiply = function(a, b)
  return tostring(bint(a) * bint(b))
end
BintUtils.divide = function(a, b)
  return tonumber(bint(a) / bint(b))
end
BintUtils.toBalanceValue = function(a)
  return tostring(bint(a))
end
BintUtils.lt = function(a, b)
  return bint(a) < bint(b)
end
BintUtils.le = function(a, b)
  return bint(a) <= bint(b)
end
BintUtils.gt = function(a, b)
  return bint(a) > bint(b)
end
BintUtils.ge = function(a, b)
  return bint(a) >= bint(b)
end
BintUtils.toNumber = function(a)
  return tonumber(a)
end

return BintUtils
