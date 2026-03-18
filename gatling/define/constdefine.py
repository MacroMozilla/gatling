from gatling.define.basedefine import BaseDefine


class ConstDefine(BaseDefine):
    """Direct-value enum for constants. Use auto() for name-only keys."""

    @staticmethod
    def _generate_next_value_(name, start, count, last_values):
        return name



# ===================== Example =====================

if __name__ == "__main__":
    from enum import auto

    class AppConfig(ConstDefine):
        Port      = 8080
        Debug     = False
        Rate      = 0.001
        Name      = "my_app"
        username  = auto()
        email     = auto()
        Single    = None

    print("=== ConstDefine ===")
    for m in AppConfig:
        print(f"  {m.name:<14} value={m.value!r}")
    print()

    # access
    print(f"  {AppConfig.Port.value = }")
    print(f"  {AppConfig.Port.name = }")
    print(f"  {str(AppConfig.Port) = }")
    print(f"  {AppConfig.username.value = }")
    print(f"  {AppConfig.Single.value = }")
    print()

    # lookup
    print(f"  {'Port' in AppConfig = }")
    print(f"  {'nope' in AppConfig = }")
    print(f"  {AppConfig['Port'] = }")
    print(f"  {AppConfig.get('Port') = }")
    print(f"  {AppConfig.get('nope') = }")
    print(f"  {AppConfig.get('nope', 'fallback') = }")
    print()

    # collection
    print(f"  {AppConfig.keys() = }")
    print(f"  {AppConfig.items() = }")
    print(f"  {dict(AppConfig) = }")
    print(f"  {len(AppConfig) = }")
