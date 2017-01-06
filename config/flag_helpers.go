package config

import "github.com/achilleasa/usrv/config/flag"

// BoolFlag creates a bool flag associated with the global config store. If a
// non-empty config path is specified, the flag will automatically update its value
// when the store value changes.
//
// For more information see the documentation for flag.NewBool
func BoolFlag(cfgPath string) *flag.BoolFlag {
	return flag.NewBool(&Store, cfgPath)
}

// Float32Flag creates a float32 flag associated with the global config store. If a
// non-empty config path is specified, the flag will automatically update its value
// when the store value changes.
//
// For more information see the documentation for flag.NewFloat32
func Float32Flag(cfgPath string) *flag.Float32Flag {
	return flag.NewFloat32(&Store, cfgPath)
}

// Float64Flag creates a float64 flag associated with the global config store. If a
// non-empty config path is specified, the flag will automatically update its value
// when the store value changes.
//
// For more information see the documentation for flag.NewFloat64
func Float64Flag(cfgPath string) *flag.Float64Flag {
	return flag.NewFloat64(&Store, cfgPath)
}

// Uint32Flag creates a uint32 flag associated with the global config store. If a
// non-empty config path is specified, the flag will automatically update its value
// when the store value changes.
//
// For more information see the documentation for flag.NewUint32
func Uint32Flag(cfgPath string) *flag.Uint32Flag {
	return flag.NewUint32(&Store, cfgPath)
}

// Uint64Flag creates a uint64 flag associated with the global config store. If a
// non-empty config path is specified, the flag will automatically update its value
// when the store value changes.
//
// For more information see the documentation for flag.NewUint64
func Uint64Flag(cfgPath string) *flag.Uint64Flag {
	return flag.NewUint64(&Store, cfgPath)
}

// Int32Flag creates a int32 flag associated with the global config store. If a
// non-empty config path is specified, the flag will automatically update its value
// when the store value changes.
//
// For more information see the documentation for flag.NewInt32
func Int32Flag(cfgPath string) *flag.Int32Flag {
	return flag.NewInt32(&Store, cfgPath)
}

// Int64Flag creates a int64 flag associated with the global config store. If a
// non-empty config path is specified, the flag will automatically update its value
// when the store value changes.
//
// For more information see the documentation for flag.NewInt64
func Int64Flag(cfgPath string) *flag.Int64Flag {
	return flag.NewInt64(&Store, cfgPath)
}

// StringFlag creates a string flag associated with the global config store. If a
// non-empty config path is specified, the flag will automatically update its value
// when the store value changes.
//
// For more information see the documentation for flag.NewString
func StringFlag(cfgPath string) *flag.StringFlag {
	return flag.NewString(&Store, cfgPath)
}

// MapFlag creates a map flag associated with the global config store. If a
// non-empty config path is specified, the flag will automatically update its value
// when the store value changes.
//
// For more information see the documentation for flag.NewMap
func MapFlag(cfgPath string) *flag.MapFlag {
	return flag.NewMap(&Store, cfgPath)
}
