#!/usr/bin/env python3
"""Entry point for the modular Tonometry controller."""

from controller_app import ControllerApp


def main():
    app = ControllerApp()
    app.mainloop()


if __name__ == "__main__":
    main()
