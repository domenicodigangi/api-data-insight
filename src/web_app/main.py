import flet as ft


class ResourcePath(ft.UserControl):
    def __init__(self, resource_name, resource_status_change, resource_delete):
        super().__init__()
        self.completed = False
        self.resource_name = resource_name
        self.resource_status_change = resource_status_change
        self.resource_delete = resource_delete

    def build(self):
        self.display_resource = ft.Checkbox(
            value=False, label=self.resource_name, on_change=self.status_changed
        )
        self.edit_name = ft.TextField(expand=1)

        self.display_view = ft.Row(
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            controls=[
                self.display_resource,
                ft.Row(
                    spacing=0,
                    controls=[
                        ft.IconButton(
                            icon=ft.icons.CREATE_OUTLINED,
                            tooltip="Edit Entry",
                            on_click=self.edit_clicked,
                        ),
                        ft.IconButton(
                            ft.icons.DELETE_OUTLINE,
                            tooltip="Delete Entry",
                            on_click=self.delete_clicked,
                        ),
                    ],
                ),
            ],
        )

        self.edit_view = ft.Row(
            visible=False,
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            controls=[
                self.edit_name,
                ft.IconButton(
                    icon=ft.icons.DONE_OUTLINE_OUTLINED,
                    icon_color=ft.colors.GREEN,
                    tooltip="Update Entry",
                    on_click=self.save_clicked,
                ),
            ],
        )
        return ft.Column(controls=[self.display_view, self.edit_view])

    async def edit_clicked(self, e):
        self.edit_name.value = self.display_resource.label
        self.display_view.visible = False
        self.edit_view.visible = True
        await self.update_async()

    async def save_clicked(self, e):
        self.display_resource.label = self.edit_name.value
        self.display_view.visible = True
        self.edit_view.visible = False
        await self.update_async()

    async def status_changed(self, e):
        self.completed = self.display_resource.value
        await self.resource_status_change(self)

    async def delete_clicked(self, e):
        await self.resource_delete(self)


class APISettingAPP(ft.UserControl):
    def __init__(self):
        super().__init__()
        self.default_resource_options = ["Option 1", "Option 2", "Option 3"]
        self.dropdown = ft.Dropdown(
            value=self.default_resource_options[0],
            options=[ft.dropdown.Option(opt) for opt in self.default_resource_options],
            on_change=self.dropdown_changed,
        )

    def build(self):
        # Add a TextField for the base API address
        self.api_address = ft.TextField(hint_text="Base API Address", expand=True)

        # Add a Checkbox for API key authentication
        self.api_key_required = ft.Checkbox(label="API Key Authentication", value=False)

        self.new_resource = ft.TextField(
            hint_text="List API Paths", on_submit=self.add_clicked, expand=True
        )
        self.resources = ft.Column()

        self.filter = ft.Tabs(
            scrollable=False,
            selected_index=0,
            on_change=self.tabs_changed,
            tabs=[ft.Tab(text="all"), ft.Tab(text="active"), ft.Tab(text="completed")],
        )

        self.items_left = ft.Text("0 items left")

        # Application's root control containing all other controls
        title_panel = ft.Column(
            width=600,
            controls=[
                ft.Row(
                    controls=[
                        ft.Text(
                            value="API Resources",
                            style=ft.TextThemeStyle.HEADLINE_MEDIUM,
                        )
                    ],
                    alignment=ft.MainAxisAlignment.CENTER,
                )
            ],
        )
        right_panel = ft.Column(
            width=600,
            controls=[
                ft.Row(
                    controls=[
                        self.api_address,  # Add base API address TextField
                        self.api_key_required,  # Add API key authentication Checkbox
                    ],
                ),
                ft.Row(
                    controls=[
                        self.new_resource,
                        ft.FloatingActionButton(
                            icon=ft.icons.ADD, on_click=self.add_clicked
                        ),
                    ],
                ),
                ft.Column(
                    spacing=25,
                    controls=[
                        self.resources,
                        ft.Row(
                            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                            vertical_alignment=ft.CrossAxisAlignment.CENTER,
                            controls=[
                                self.items_left,
                                ft.OutlinedButton(
                                    text="Clear completed", on_click=self.clear_clicked
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        )

        # Add the dropdown for selecting default resources
        left_panel = ft.Column(width=600, controls=[self.dropdown])

        # Combine the left panel (dropdown) and the main resource panel
        main_layout = ft.Column(
            width=600,
            controls=[title_panel, ft.Row(controls=[left_panel, right_panel])],
        )

        return main_layout

    async def dropdown_changed(self, e):
        self.resources.controls.clear()
        selected_option = e.control.value
        # Logic to determine which resources to display based on the selected option
        if selected_option == "Option 1":
            default_resources = ["Resource 1", "Resource 2", "Resource 3"]
        elif selected_option == "Option 2":
            default_resources = ["Resource 4", "Resource 5", "Resource 6"]
        else:  # Option 3 or any other option
            default_resources = ["Resource 7", "Resource 8", "Resource 9"]

        for resource_name in default_resources:
            resource = ResourcePath(
                resource_name,
                self.resource_status_change,
                self.resource_delete,
            )
            self.resources.controls.append(resource)
        await self.update_async()

    async def add_clicked(self, e):
        if self.new_resource.value:
            resource = ResourcePath(
                self.new_resource.value,
                self.resource_status_change,
                self.resource_delete,
            )
            self.resources.controls.append(resource)
            self.new_resource.value = ""
            await self.new_resource.focus_async()
            await self.update_async()

    async def resource_status_change(self, resource):
        await self.update_async()

    async def resource_delete(self, resource):
        self.resources.controls.remove(resource)
        await self.update_async()

    async def tabs_changed(self, e):
        await self.update_async()

    async def clear_clicked(self, e):
        for resource in self.resources.controls[:]:
            if resource.completed:
                await self.resource_delete(resource)

    async def update_async(self):
        status = self.filter.tabs[self.filter.selected_index].text
        count = 0
        for resource in self.resources.controls:
            if not resource.completed:
                count += 1
        self.items_left.value = f"{count} active item(s) left"
        await super().update_async()


async def main(page: ft.Page):
    page.title = "API Data Insights"
    page.horizontal_alignment = ft.CrossAxisAlignment.CENTER
    page.scroll = ft.ScrollMode.ADAPTIVE

    # Create app control and add it to the page
    await page.add_async(APISettingAPP())


if __name__ == "__main__":
    ft.app(target=main, view=ft.WEB_BROWSER)
